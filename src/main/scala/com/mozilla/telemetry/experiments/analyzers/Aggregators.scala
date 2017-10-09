package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.experiments.intermediates.PreAggregateRow
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.Map


abstract class MetricAggregator[T]
  extends Aggregator[PreAggregateRow[T], Map[T, Long], Map[Long, HistogramPoint]] {
  def zero: Map[T, Long] = Map[T, Long]()

  def reduce(b: Map[T, Long], s: PreAggregateRow[T]): Map[T, Long] = {
    s.metric match {
      case Some(m: Map[T, Long]) => addHistograms[T](b, m)
      case _ => b
    }
  }

  def merge(l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = addHistograms[T](l, r)

  def outputEncoder: Encoder[Map[Long, HistogramPoint]] = ExpressionEncoder()
}

trait BooleanAggregatorTrait {
  def finish(b: Map[Boolean, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    val f = b.getOrElse(false, 0L).toDouble
    val t = b.getOrElse(true, 0L).toDouble

    Map(0L -> HistogramPoint(f/sum, f, Some("False")), 1L -> HistogramPoint(t/sum, t, Some("True")))
  }

  def bufferEncoder: Encoder[Map[Boolean, Long]] = ExpressionEncoder()
}

object BooleanAggregator extends MetricAggregator[Boolean] with BooleanAggregatorTrait

object UintAggregator extends MetricAggregator[Int] {
  def finish(b: Map[Int, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    b.map { case (k: Int, v) => k.toLong -> HistogramPoint(v.toDouble / sum, v.toDouble, None) }
  }

  def bufferEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()
}

trait LongAggregatorTrait {
  def finish(b: Map[Long, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    b.map { case (k: Long, v) => k -> HistogramPoint(v.toDouble / sum, v.toDouble, None) }
  }

  def bufferEncoder: Encoder[Map[Long, Long]] = ExpressionEncoder()
}

object LongAggregator extends MetricAggregator[Long] with LongAggregatorTrait

trait StringAggregatorTrait {
  def finish(b: Map[String, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    // We can't assign real numeric indexes until we combine all the histograms across all branches
    // so just assign any number for now
    b.zipWithIndex.map {
      case ((l: String, v), i) => i.toLong -> HistogramPoint(v.toDouble / sum, v.toDouble, Some(l))
    }
  }

  def bufferEncoder: Encoder[Map[String, Long]] = ExpressionEncoder()
}

object StringAggregator extends MetricAggregator[String] with StringAggregatorTrait

abstract class IntermediateAggregator[T]
  extends Aggregator[MetricAnalysis, Map[T, Long], Map[Long, HistogramPoint]] {
  def zero: Map[T, Long] = Map[T, Long]()

  protected def transformHistogram(in: (Long, HistogramPoint)): (T, Long)

  def reduce(b: Map[T, Long], s: MetricAnalysis): Map[T, Long] = {
    addHistograms[T](b, s.histogram.map(transformHistogram))
  }

  def merge(l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = addHistograms[T](l, r)

  def outputEncoder: Encoder[Map[Long, HistogramPoint]] = ExpressionEncoder()
}

trait UsesStandardTransform {
  protected def transformHistogram(in: (Long, HistogramPoint)): (Long, Long) = {
    in match {
      case (k, v) => k -> v.count.toLong
    }
  }
}

object BooleanIntermediateAggregator
  extends IntermediateAggregator[Boolean] with BooleanAggregatorTrait {
  override protected def transformHistogram(in: (Long, HistogramPoint)): (Boolean, Long) = {
    in match {
      case (0, v) => false -> v.count.toLong
      case (1, v) => true -> v.count.toLong
      case _ => throw new Exception("Invalid boolean metric")
    }
  }
}

object LongIntermediateAggregator
  extends IntermediateAggregator[Long] with LongAggregatorTrait with UsesStandardTransform

object StringIntermediateAggregator
  extends IntermediateAggregator[String] with StringAggregatorTrait {
  override protected def transformHistogram(in: (Long, HistogramPoint)): (String, Long) = {
    in match {
      case (_, v) => v.label.getOrElse("") -> v.count.toLong
    }
  }
}
