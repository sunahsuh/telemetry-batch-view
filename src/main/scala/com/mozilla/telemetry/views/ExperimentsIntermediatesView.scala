package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.Permutations.weightedGenerator
import com.mozilla.telemetry.experiments.analyzers.MetricAnalysis
import com.mozilla.telemetry.experiments.intermediates._
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.functions.{col, countDistinct, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf


object ExperimentsIntermediatesView {
  def jobName = "experiments_intermediates"
  def schemaVersion = "v1"
  // This gives us ~120MB partitions with the columns we have now. We should tune this as we add more columns.
  def rowsPerPartition = 25000

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  // scalar_ and histogram_ columns are automatically added
  private val usedColumns = List(
    "client_id",
    "experiment_id",
    "experiment_branch"
  )

  implicit class ExperimentDataFrame(df: DataFrame) {
    def selectUsedColumns: DataFrame = {
      val columns = (usedColumns
        ++ df.schema.map(_.name).filter { s: String => s.startsWith("histogram_") || s.startsWith("scalar_") })
      df.select(columns.head, columns.tail: _*)
    }
  }

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for parquet data", required = true)
    val bucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)
    verify()
  }

  def getSpark: SparkSession = {
    val spark = getOrCreateSparkSession(jobName)
    if (spark.sparkContext.defaultParallelism > 200) {
      spark.sqlContext.sql(s"set spark.sql.shuffle.partitions=${spark.sparkContext.defaultParallelism}")
    }
    spark.sparkContext.setLogLevel("INFO")
    spark
  }


  def main(args: Array[String]) {
    val conf = new Conf(args)
    val sparkSession = getSpark
    val date = getDate(conf)
    val inputBucket = conf.inputBucket.get.getOrElse(conf.bucket)
    val inputLocation = s"s3://$inputBucket/${ExperimentSummaryView.datasetPath}/"

    val experimentData = sparkSession.read.parquet(inputLocation)
    logger.info("=======================================================================================")
    logger.info(s"Starting $jobName for date $date")

    val experiments = getExperiments(conf, experimentData, date)
    sparkSession.stop()

    logger.info(s"List of experiments to process for $date is: $experiments")

    experiments.foreach{ e: String =>
      logger.info(s"Aggregating pings for experiment $e from date $date")

      val spark = getSpark

      val experimentsSummary = spark.read.parquet(inputLocation)
        .where(col("experiment_id") === e)
        .where(col("submission_date_s3") === date)

      val outputLocation = s"s3://${conf.bucket()}/$datasetPath/experiment_id=$e/submission_date_s3=$date"

      import spark.implicits._
      getAggregates(e, experimentsSummary, conf)
        .toDF()
        .drop(col("experiment_id"))
        .repartition(1)
        .write.mode("overwrite").parquet(outputLocation)

      logger.info(s"Wrote intermediates to $outputLocation")
      spark.stop()
    }
  }

  def getExperiments(conf: Conf, data: DataFrame, date: String): List[String] = {
    conf.experiment.get match {
      case Some(e) => List(e)
      case _ => data // get all experiments
        .where(col("submission_date_s3") === date)
        .select("experiment_id")
        .distinct()
        .collect()
        .toList
        .map(r => r(0).asInstanceOf[String])
    }
  }

  def getDate(conf: Conf): String =  {
    conf.date.get match {
      case Some(d) => d
      case _ => com.mozilla.telemetry.utils.yesterdayAsYYYYMMDD
    }
  }

  def getMetrics(conf: Conf, data: DataFrame) = {
    conf.metric.get match {
      case Some(m) =>
        List((m, (Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _) ++ Scalars.definitions())(m.toUpperCase)))
      case _ => {
        val histogramDefs = MainSummaryView.filterHistogramDefinitions(
          Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _),
          useWhitelist = true)
        val scalarDefs = Scalars.definitions(includeOptin = true).toList
        scalarDefs ++ histogramDefs
      }
    }
  }

  def getAggregates(experiment: String, experimentsSummary: DataFrame, conf: Conf): List[MetricAnalysis] = {
    val persisted = addPermutationsAndPersist(experimentsSummary, experiment)

    val metricList = getMetrics(conf, experimentsSummary)

    metricList.flatMap {
      case (name: String, md: MetricDefinition) =>
        md match {
          case hd: HistogramDefinition =>
            new HistogramIntermediate(name, hd, persisted).aggregate.collect()
          case sd: ScalarDefinition =>
            ScalarIntermediate.getIntermediateAggregator(name, sd, persisted).aggregate.collect()
          case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
        }
    }
  }

  // Adds a column for permutations, repartitions if warranted, and persists the result for quicker access
  def addPermutationsAndPersist(experimentsSummary: DataFrame, experiment: String): DataFrame = {
    val totalPings = experimentsSummary.count()
    // Since we're be saving daily intermediate aggregates permutations this is going to pose a bit of an issue
    // since we won't have stable cutoffs for permutations -- some (hopefully small) number of clients will switch
    // branches between days in their permutations, but I don't think that should have a significant impact (since,
    // after all, we're testing the null hypothesis, i.e. we're positing there's no difference)
    val clientsPerBranch = experimentsSummary
      .select("experiment_branch", "client_id")
      .groupBy("experiment_branch")
      .agg(countDistinct("client_id"))
      .collect
      .map(r => r.getString(0) -> r.getLong(1))
      .toMap

    val withPermutations = addPermutations(experimentsSummary.selectUsedColumns, clientsPerBranch, experiment)
    (totalPings / rowsPerPartition match {
      case c if c > experimentsSummary.sparkSession.sparkContext.defaultParallelism =>
        withPermutations.repartition(c.toInt)
      case _ =>
        withPermutations
    }).persist(StorageLevel.MEMORY_AND_DISK)
  }

  def addPermutations(df: DataFrame, branchCounts: Map[String, Long], experiment: String): DataFrame = {
    // create the cutoffs from branchCounts
    val total = branchCounts.values.sum.toDouble
    val cutoffs = branchCounts
      .toList
      .sortBy(_._1) // sort by branch name
      .scanLeft(0L)(_ + _._2) // transform to cumulative branch counts
      .tail  // drop the initial 0 from scanLeft
      .map(_/total) // turn cumulative counts into cutoffs from 0 to 1

    val generator = weightedGenerator(cutoffs, experiment, numPermutations = 100) _
    val permutationsUDF = udf(generator)
    df.withColumn("permutations", permutationsUDF(col("client_id")))
  }

  def datasetPath: String = {
    s"$jobName/$schemaVersion/"
  }
}
