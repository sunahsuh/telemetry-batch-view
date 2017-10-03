package com.mozilla.telemetry.experiments.intermediates

import com.mozilla.telemetry.experiments.analyzers._
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql._

import scala.collection.Map


// Spark doesn't support Datasets of case classes with type parameters (nor of abstract type members) -- otherwise we'd
// be able to avoid creating the concrete type versions of all of these case classes

trait definesToScalarMapRow[R] {
  def toScalarMapRow: R
}

trait ScalarRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[T]

  def scalarMapRowMetric: Option[Map[T, Long]] = {
    metric match {
      case Some(m) => Some(Map(m -> 1L))
      case _ => None
    }
  }
}

case class BooleanScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Boolean])
  extends ScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class UintScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Int])
  extends ScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class LongScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Long])
  extends ScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class StringScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[String])
  extends ScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}

trait KeyedScalarRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[String, T]]

  def collapsedMetric: Option[Map[T, Long]] = {
    metric match {
      case Some(m: Map[String, T]) => Some(m.values.foldLeft(Map.empty[T, Long]) {
        case(m: Map[T, Long], e: T @unchecked) => m + (e -> (m.getOrElse(e, 0L) + 1L))
      })
      case _ => None
    }
  }
}

case class KeyedBooleanScalarRow(experiment_id: String, branch: String, subgroup: String,
                                 metric: Option[Map[String, Boolean]])
  extends KeyedScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedUintScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Int]])
  extends KeyedScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedLongScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]])
  extends KeyedScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedStringScalarRow(experiment_id: String, branch: String, subgroup: String,
                                metric: Option[Map[String, String]])
  extends KeyedScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}

case class BooleanScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                               metric: Option[Map[Boolean, Long]]) extends PreAggregateRow[Boolean]
case class UintScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Int, Long]]) extends PreAggregateRow[Int]
case class LongScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Long, Long]]) extends PreAggregateRow[Long]
case class StringScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]]) extends PreAggregateRow[String]


class BooleanScalarIntermediate(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricIntermediate[Boolean](name, md, df) {
  override type PreAggregateRowType = BooleanScalarMapRow
  val aggregator = BooleanAggregator

  override def validateRow(row: BooleanScalarMapRow): Boolean = row.metric.isDefined

  def collapseKeys(formatted: DataFrame): Dataset[BooleanScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedBooleanScalarRow] else formatted.as[BooleanScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class UintScalarIntermediate(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricIntermediate[Int](name, md, df) {
  override type PreAggregateRowType = UintScalarMapRow
  val aggregator = UintAggregator
  override def validateRow(row: UintScalarMapRow): Boolean = row.metric match {
    case Some(m) => m.keys.forall(_ >= 0)
    case None => false
  }

  def collapseKeys(formatted: DataFrame): Dataset[UintScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedUintScalarRow] else formatted.as[UintScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class LongScalarIntermediate(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricIntermediate[Long](name, md, df) {
  override type PreAggregateRowType = LongScalarMapRow
  val aggregator = LongAggregator

  override def validateRow(row: LongScalarMapRow): Boolean = row.metric match {
    case Some(m) => m.keys.forall(_ >= 0L)
    case None => false
  }

  def collapseKeys(formatted: DataFrame): Dataset[LongScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedLongScalarRow] else formatted.as[LongScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class StringScalarIntermediate(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricIntermediate[String](name, md, df) {
  override type PreAggregateRowType = StringScalarMapRow
  val aggregator = StringAggregator

  override def validateRow(row: StringScalarMapRow): Boolean = row.metric.isDefined

  def collapseKeys(formatted: DataFrame): Dataset[StringScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedStringScalarRow] else formatted.as[StringScalarRow]
    s.map(_.toScalarMapRow)
  }
}

object ScalarIntermediate {
  def getIntermediateAggregator(name: String, sd: ScalarDefinition, df: DataFrame): MetricIntermediate[_] = {
    sd match {
      case s: BooleanScalar => new BooleanScalarIntermediate(name, s, df)
      case s: UintScalar => new UintScalarIntermediate(name, s, df)
      case s: StringScalar => new StringScalarIntermediate(name, s, df)
      case _ => throw new UnsupportedOperationException("Unsupported scalar type")
    }
  }
}