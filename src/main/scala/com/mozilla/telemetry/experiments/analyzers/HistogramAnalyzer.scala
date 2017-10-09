package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics.HistogramDefinition
import org.apache.spark.sql._


class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame)
  extends MetricAnalyzer[Long](name, hd, df) {
  override val aggregator = LongIntermediateAggregator
}
