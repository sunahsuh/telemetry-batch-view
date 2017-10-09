package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.experiments.intermediates.MetricKey
import com.mozilla.telemetry.metrics.MetricDefinition
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, count, lit}

import scala.collection.Map
import scala.util.{Failure, Success, Try}


// pdf is the count normalized by counts for all buckets
case class HistogramPoint(pdf: Double, count: Double, label: Option[String])

case class Statistic(comparison_branch: Option[String],
                     name: String,
                     value: Double,
                     confidence_low: Option[Double] = None,
                     confidence_high: Option[Double] = None,
                     confidence_level: Option[Double] = None,
                     p_value: Option[Double] = None)

case class MetricAnalysis(experiment_id: String,
                          experiment_branch: String,
                          subgroup: String,
                          n: Long,
                          metric_name: String,
                          metric_type: String,
                          histogram: Map[Long, HistogramPoint],
                          statistics: Option[Seq[Statistic]])

abstract class MetricAnalyzer[T](name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  val aggregator: IntermediateAggregator[T]
  import df.sparkSession.implicits._

  def analyze(): Dataset[MetricAnalysis] = {
    val agg_column = aggregator.toColumn.name("metric_aggregate")
    val output = asDataset
      .groupByKey(x => MetricKey(x.experiment_id, x.experiment_branch, x.subgroup))
      .agg(agg_column, count("*"))
      .map(toOutputSchema _)
    reindex(output)
  }

  private def asDataset: Dataset[MetricAnalysis] = {
    df.drop("submission_date_s3").as[MetricAnalysis].filter(_.metric_name == name)
  }

  protected def reindex(aggregates: Dataset[MetricAnalysis]): Dataset[MetricAnalysis] = {
    // This is meant as a finishing step for string scalars only
    aggregates
  }

  private def toOutputSchema(r: (MetricKey, Map[Long, HistogramPoint], Long)): MetricAnalysis = r match {
    case (k: MetricKey, h: Map[Long, HistogramPoint], n: Long) =>
      MetricAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, md.getClass.getSimpleName, h, None)
  }
}
