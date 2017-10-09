package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics._
import org.apache.spark.sql._


class BooleanScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Boolean](name, md, df) {
  val aggregator = BooleanIntermediateAggregator
}

class UintScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Long](name, md, df) {
  val aggregator = LongIntermediateAggregator
}

class LongScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Long](name, md, df) {
  val aggregator = LongIntermediateAggregator
}

class StringScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[String](name, md, df) {
  val aggregator = StringIntermediateAggregator

  override protected def reindex(aggregates: Dataset[MetricAnalysis]): Dataset[MetricAnalysis] = {
    import aggregates.sparkSession.implicits._
    // this is really annoying, but we need to give string scalar values indexes and they have to be
    // consistent among all the histograms across all branches, so we: aggregate the histograms
    // across all the branches, sort by count descending, and use that order for our index
    val counts = aggregates.collect().map { a: MetricAnalysis =>
      a.histogram.values.map {p: HistogramPoint => p.label.get -> p.count.toLong}.toMap[String, Long]
    }

    if (counts.isEmpty)
      aggregates
    else {
      val indexes = counts.reduce(addHistograms[String]).toSeq.sortWith(_._2 > _._2).zipWithIndex.map {
        case ((key, _), index) => key -> index.toLong
      }.toMap

      aggregates.map(r => r.copy(
        histogram = r.histogram.map { case (_, p) => indexes(p.label.get) -> p }
      ))
    }
  }
}

object ScalarAnalyzer {
  def getAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame): MetricAnalyzer[_] = {
    sd match {
      case s: BooleanScalar => new BooleanScalarAnalyzer(name, s, df)
      case s: UintScalar => new LongScalarAnalyzer(name, s, df)
      case s: StringScalar => new StringScalarAnalyzer(name, s, df)
      case _ => throw new UnsupportedOperationException("Unsupported scalar type")
    }
  }
}
