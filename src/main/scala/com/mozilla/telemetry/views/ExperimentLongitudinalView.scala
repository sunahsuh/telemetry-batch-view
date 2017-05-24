package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.Dataset
import com.mozilla.telemetry.utils.S3Store
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import com.mozilla.telemetry.views.LongitudinalView

object ExperimentLongitudinalView {
  val jobName = "experiment_longitudinal"
  val schemaVersion = "v1"

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String]("bucket", descr = "bucket", required = true)
    val telemetrySource = opt[String]("source", descr = "Source for Dataset.from_source. Defaults to telemetry-cohorts.", required = false)
    val experimentId = opt[String]("experiment", descr = "Experiment name to add to as a field", required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val telemetrySource = opts.telemetrySource.get match {
      case Some(ts) => ts
      case _ => "telemetry-cohorts"
    }
    val experimentId = opts.experimentId()

    val sparkConf = new SparkConf().setAppName("Longitudinal")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)

    val messages = Dataset(telemetrySource)
      .where("docType") {
        case "main" => true
      }.where("experimentId") {
        case e if e == experimentId => true
      }

    val prefix = s"$jobName/$schemaVersion/experiment=$experimentId"
    val bucket = opts.outputBucket()

    cleanS3Prefix(bucket, prefix)

    val clientMessages = LongitudinalView.sortMessages(messages)
    val counts = LongitudinalView.processAndSave(clientMessages, false, bucket, prefix, Some(experimentId))

    println("Clients seen: %d".format(counts._1))
    println("Clients ignored: %d".format(counts._2))

    sc.stop()
  }

  def cleanS3Prefix(bucket: String, prefix: String): Unit = {
    // We should probably speed this up by doing batch calls
    // and possibly move this into the moztelemetry S3Store object
    while (!S3Store.isPrefixEmpty(bucket, prefix)) {
      S3Store.listKeys(bucket, prefix).foreach {
        summary => S3Store.deleteKey(bucket, summary.key)
      }
    }
  }
}