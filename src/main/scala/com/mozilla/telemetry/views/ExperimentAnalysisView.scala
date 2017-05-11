package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.MetricAnalyzer
import com.mozilla.telemetry.histograms.{HistogramDefinition, Histograms}
import com.mozilla.telemetry.scalars.{ScalarDefinition, Scalars}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.types._

object ExperimentAnalysisView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiment_analysis"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    /*
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    */
    // val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    /*
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    val experiment = opt[String]("experiment", descr = "Only process data from the given experiment", required = false)
    verify()
    */
    val hist = opt[String]("histogram", descr = "Run job on just this histogram", required = false)
    verify()
  }

  def main(args: Array[String]) {
    // Spark/job setup stuff
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(jobName)
      .getOrCreate()

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // DEBUG -- remove before committing
    spark.sparkContext.setLogLevel("WARN")
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    // Grab messages
    val rows = getRows(spark)

    val histogramList = conf.hist.get match {
      case Some(h) => Map(h -> Histograms.definitions()(h.toUpperCase))
      case _ => Histograms.definitions()
    }
    val output = histogramList.flatMap {
      case(name: String, hd: HistogramDefinition) =>
        MetricAnalyzer.getAnalyzer(
          name.toLowerCase, hd, rows
        ).analyze
      case _ => List()
    } ++
    Scalars.definitions().flatMap {
      case(name: String, sd: ScalarDefinition) =>
        MetricAnalyzer.getAnalyzer(
          Scalars.getParquetFriendlyScalarName(name.toLowerCase, "parent"), sd, rows
        ).analyze
    }
    output.foreach(println)

    //val o = spark.sparkContext.parallelize(output.toList)
    //val df = spark.sqlContext.createDataFrame(o, buildOutputSchema)
    // df.repartition(1).write.parquet(s"s3://telemetry-test-bucket/ssuh/fake_experiment_analysis2")
    // df.repartition(1).write.parquet("/Users/ssuh/dev/mozilla/scratchnalysis_output")
    spark.stop()
  }

  def getRows(spark: SparkSession): DataFrame = {
    // spark.read.parquet(s"s3://telemetry-test-bucket/ssuh/fake_button_change_experiment/")
    spark.read.parquet("/Users/ssuh/dev/mozilla/scratch/longitudinal_shard_fake_branches")
  }

  def buildStatisticSchema = StructType(List(
    StructField("comparison_branch", StringType, nullable = true),
    StructField("name", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false),
    StructField("confidence_low", DoubleType, nullable = true),
    StructField("confidence_high", DoubleType, nullable = true),
    StructField("confidence_level", DoubleType, nullable = true),
    StructField("p_value", DoubleType, nullable = true)
  ))

  def buildHistogramPointSchema = StructType(List(
    StructField("pdf", DoubleType),
    StructField("count", LongType),
    StructField("label", StringType, nullable = true)
  ))

  def buildOutputSchema = StructType(List(
    StructField("experiment_name", StringType, nullable = false),
    StructField("experiment_branch", StringType, nullable = false),
    StructField("subgroup", StringType, nullable = true),
    StructField("n", LongType, nullable = false),
    StructField("metric_name", StringType, nullable = false),
    StructField("metric_type", StringType, nullable = false),
    StructField("histogram", MapType(LongType, buildHistogramPointSchema), nullable = false),
    StructField("statistics", ArrayType(buildStatisticSchema, containsNull = false), nullable = true)
  ))
}