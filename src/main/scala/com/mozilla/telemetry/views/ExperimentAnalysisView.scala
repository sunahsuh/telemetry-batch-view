package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers._
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.functions.{col, min}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success, Try}

object ExperimentAnalysisView {
  def defaultErrorAggregatesBucket = "net-mozaws-prod-us-west-2-pipeline-data"
  def errorAggregatesPath = "error_aggregates/v2"
  def jobName = "experiment_analysis"
  def schemaVersion = "v1"

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val bucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for parquet data", required = true)
    val errorAggregatesBucket = opt[String](descr = "Bucket for error_aggregates data", required = false,
      default = Some(defaultErrorAggregatesBucket))
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
    val inputBucket = conf.inputBucket.get.getOrElse(conf.bucket)
    val experimentsSummaryLocation = s"s3://$inputBucket/${ExperimentSummaryView.datasetPath}/"
    val experimentsIntermediatesLocation = s"s3://$inputBucket/${ExperimentsIntermediatesView.datasetPath}/"

    val experimentsData = sparkSession.read.option("mergeSchema", "true").parquet(experimentsIntermediatesLocation)
    val date = getDate(conf)
    logger.info("=======================================================================================")
    logger.info(s"Starting $jobName for date $date")

    val experiments = getExperiments(conf, experimentsData)
    sparkSession.stop()

    logger.info(s"List of experiments to process for $date is: $experiments")

    experiments.foreach{ e: String =>
      logger.info(s"Aggregating pings for experiment $e")

      val spark = getSpark

      val experimentsIntermediates = spark.read.option("mergeSchema", "true").parquet(experimentsIntermediatesLocation)

      val experimentsSummary = spark.read.option("mergeSchema", "true").parquet(experimentsSummaryLocation)
        .where(col("experiment_id") === e)

      val minDate = experimentsSummary
        .agg(min("submission_date_s3"))
        .first.getAs[String](0)

      val errorAggregates = Try(spark.read.parquet(s"s3://${conf.errorAggregatesBucket()}/$errorAggregatesPath")) match {
        case Success(df) => df.where(col("experiment_id") === e && col("submission_date") >= minDate)
        case Failure(_) => spark.emptyDataFrame
      }

      val outputLocation = s"${conf.bucket()}/$datasetPath/experiment_id=$e/date=$date"

      import spark.implicits._
      getExperimentMetrics(e, experimentsIntermediates, experimentsSummary, errorAggregates, conf)
        .toDF()
        .drop(col("experiment_id"))
        .repartition(1)
        .write.mode("overwrite").parquet(outputLocation)

      logger.info(s"Wrote aggregates to $outputLocation")
      spark.stop()
    }
  }

  def getDate(conf: Conf): String =  {
   conf.date.get match {
      case Some(d) => d
      case _ => com.mozilla.telemetry.utils.yesterdayAsYYYYMMDD
    }   
  }

  def getExperiments(conf: Conf, data: DataFrame): List[String] = {
    conf.experiment.get match {
      case Some(e) => List(e)
      case _ => data // get all experiments
        .where(col("submission_date_s3") === getDate(conf))
        .select("experiment_id")
        .distinct()
        .collect()
        .toList
        .map(r => r(0).asInstanceOf[String])
    }
  }

  def getMetrics(conf: Conf, data: DataFrame): List[(String, MetricDefinition)] = {
    val fromData = data.select("metric_name").distinct().collect.map(_.getString(0))
    conf.metric.get match {
      case Some(m) => {
        List((m, (Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _) ++ Scalars.definitions())(m.toUpperCase)))
      }
      case _ => {
        val histogramDefs = MainSummaryView.filterHistogramDefinitions(
          Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _),
          useWhitelist = true)
        val scalarDefs = Scalars.definitions(includeOptin = true).toList
        scalarDefs ++ histogramDefs
      }.filter {m: (String, MetricDefinition) => fromData.contains(m._1)}
    }
  }

  def getExperimentMetrics(experiment: String, experimentsIntermediates: DataFrame, experimentsSummary: DataFrame, errorAggregates: DataFrame, conf: Conf): List[MetricAnalysis] = {
    val metadata = ExperimentAnalyzer.getExperimentMetadata(experimentsSummary).collect()

    val metricList = getMetrics(conf, experimentsSummary)

    val metrics = metricList.flatMap {
      case (name: String, md: MetricDefinition) =>
        md match {
          case hd: HistogramDefinition =>
            new HistogramAnalyzer(name, hd, experimentsIntermediates).analyze().collect()
          case sd: ScalarDefinition =>
            ScalarAnalyzer.getAnalyzer(name, sd, experimentsIntermediates).analyze().collect()
          case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
        }
    }

    val crashes = CrashAnalyzer.getExperimentCrashes(errorAggregates)
    (metadata ++ metrics ++ crashes).toList
  }

  def datasetPath: String = {
    s"$jobName/$schemaVersion/"
  }
}
