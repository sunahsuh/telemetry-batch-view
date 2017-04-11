package com.mozilla.telemetry.views

import org.apache.spark.sql.{SparkSession}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions._

import scala.util.hashing.MurmurHash3

object CreateFakeExperimentView {
  def schemaVersion: String = "v1"
  def jobName: String = "create_fake_experiment"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val input = opt[String]("input", descr = "Input parquet file(s) location in the shape of longitudinal data", required = true)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of rows to tag", required = false)
    val experimentName = opt[String]("experiment", descr = "Fake experiment name", required = true)
    val branches = opt[List[String]]("branch", descr = "Branches of experiment", required = true)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(jobName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val longitudinal = spark.read.parquet(conf.input())
    val branches = conf.branches()
    val numBranches = branches.size
    val assignBranch: String => String = c => branches(MurmurHash3.stringHash(c, 123).abs % numBranches)
    val branchUDF = udf(assignBranch)
    val tagged = longitudinal
      .limit(conf.limit.get.getOrElse(1000))
      .withColumn("experiment", lit(conf.experimentName()))
      .withColumn("branch", branchUDF(col("client_id")))

    tagged.repartition(10).write.parquet(s"s3://${conf.outputBucket()}/ssuh/${conf.experimentName()}")
    spark.stop()
  }
}