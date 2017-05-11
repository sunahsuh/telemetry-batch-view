package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.scalars.{ScalarDefinition, StringScalar, UintScalar}
import com.mozilla.telemetry.utils.userdefinedfunctions.AggregateScalars
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row}


sealed abstract class ScalarAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame) extends MetricAnalyzer(name, sd, df) {
  def handleKeys: Column = if (sd.keyed) keyedUDF(col("metric")) else col(s"metric.value")
}

class UintScalarAnalyzer(name: String, sd: UintScalar, df: DataFrame) extends ScalarAnalyzer(name, sd, df) {
  val reducer = new AggregateScalars[Long](LongType)
  lazy val keyedUDF = udf(collapseKeys _)
  val getAggregateKeys = udf[Seq[Long], Map[Long, Row]](_.keys.toSeq)

  def analyze: List[Row] = {
    println(name)
    val filtered = formatAndFilter match {
      case Some(x: DataFrame) => x.persist()
      case _ => return List()
    }
    val aggregates = aggregate(explodeMetric(filtered)).persist()
    val withSummaries = runSummaryStatistics(aggregates)
    val rows = runTestStatistics(filtered, withSummaries)
    // TODO: test if unpersist is lazy the same way persist is -- if not, this is meaningless
    filtered.unpersist()
    aggregates.unpersist()
    rows.collect().toList
  }

  def collapseKeys(m: Map[String, Seq[Row]]): Seq[Long] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(_.map {
        case Row(v: Long) => v
        case _ => 0L
      }).map(_.sum).toSeq
    }
  }
}



class StringScalarAnalyzer(name: String, sd: StringScalar, df: DataFrame) extends ScalarAnalyzer(name, sd, df) {
  def analyze: List[Row] = {
    println(name)
    val filtered = formatAndFilter match {
      case Some(x: DataFrame) => x.persist()
      case _ => return List()
    }
    val aggregates = aggregate(explodeMetric(filtered)).persist()
    val withSummaries = runSummaryStatistics(aggregates)
    val rows = runTestStatistics(filtered, withSummaries).collect().toList
    keyMapping(aggregates)
    filtered.unpersist()
    aggregates.unpersist()
    rows
  }

  val reducer = new AggregateScalars[String](StringType)

  def collapseKeys(m: Map[String, Seq[Row]]): Seq[String] = {
    m match {
      case null => List()
      case _ => m.values.transpose.flatMap(_.map {
        case Row(v: String) => v
        case _ => null
      }).toSeq
    }
  }
  lazy val keyedUDF = udf(collapseKeys _)
  val getAggregateKeys = udf[Seq[String], Map[String, Row]](_.keys.toSeq)

  def keyMapping(aggregate: DataFrame) = {
    aggregate
      .select(explode(col("histogram_agg")))
      .groupBy("key")
      .agg(sum(col("value")).as("sum"))
      .orderBy(desc("sum"))
      .select("key")
      .collect()
      .map(_.get(0))
      .zipWithIndex
      .map { case (k, v) => k -> v }
      .toMap
      .foreach(println)
  }

}
