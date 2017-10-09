package com.mozilla.telemetry.experiments.intermediates

import com.mozilla.telemetry.experiments.analyzers.{MetricAggregator, HistogramPoint, MetricAnalysis}
import com.mozilla.telemetry.metrics.MetricDefinition
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, count, lit}

import scala.collection.Map
import scala.util.{Failure, Success, Try}

trait PreAggregateRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[T, Long]]
}

case class MetricKey(experiment_id: String, branch: String, subgroup: String)

abstract class MetricIntermediate[T](name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  type PreAggregateRowType <: PreAggregateRow[T]
  val aggregator: MetricAggregator[T]
  def validateRow(row: PreAggregateRowType): Boolean

  import df.sparkSession.implicits._

  def aggregate: Dataset[MetricAnalysis] = {
    format match {
      case Some(d: DataFrame) =>
        val agg_column = aggregator.toColumn.name("metric_aggregate")
        collapseKeys(d)
          .filter(validateRow _)
          .groupByKey(x => MetricKey(x.experiment_id, x.branch, x.subgroup))
          .agg(agg_column, count("*"))
          .map(toOutputSchema)
      case _ => df.sparkSession.emptyDataset[MetricAnalysis]
    }
  }

  private def format: Option[DataFrame] = {
    Try(df.select(
      col("experiment_id"),
      col("experiment_branch").as("branch"),
      lit("All").as("subgroup"),
      col(name).as("metric"))
    ) match {
      case Success(x) => Some(x)
      // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
      case Failure(_: org.apache.spark.sql.AnalysisException) => None
      case Failure(x: Throwable) => throw x
    }
  }

  def collapseKeys(formatted: DataFrame): Dataset[PreAggregateRowType]

  private def toOutputSchema(r: (MetricKey, Map[Long, HistogramPoint], Long)): MetricAnalysis = r match {
    case (k: MetricKey, h: Map[Long, HistogramPoint], n: Long) =>
      MetricAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, md.getClass.getSimpleName, h, None)
  }
}
