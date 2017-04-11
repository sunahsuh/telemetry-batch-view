package com.mozilla.telemetry.utils.userdefinedfunctions

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class AggregateScalars[T](U: DataType) extends UserDefinedAggregateFunction {
  def add(m: Map[T, Long], k: T): Map[T, Long] = {
    m + (k -> (m.getOrElse(k, 0L) + 1L))
  }

  def combine(l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = {
    l ++ r.map {case (k, v) => k -> (l.getOrElse(k, 0L) + v)}
  }

  override def inputSchema: StructType =
    StructType(StructField("value", U) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("aggregate", MapType(U, LongType)) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = MapType(U, LongType)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[T, Long]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val m = buffer.getAs[Map[T, Long]](0)
    val k = input.getAs[T](0)
    buffer(0) = add(m, k)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = combine(buffer1.getAs[Map[T, Long]](0), buffer2.getAs[Map[T, Long]](0))
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Map[T, Long]](0)
  }
}

class AggregateHistograms(histogramLength: Int) extends UserDefinedAggregateFunction {
  private def aggregateArrays(left: Seq[Long], right: Seq[Long]): Array[Long] = {
    left.zip(right).map(x => x._1 + x._2).toArray
  }

  override def inputSchema: StructType =
    StructType(StructField("value", ArrayType(IntegerType)) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("aggregate", ArrayType(LongType)) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = ArrayType(LongType)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array.fill[Long](histogramLength)(0L)
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = aggregateArrays(buffer.getAs[Seq[Long]](0), input.getAs[Seq[Int]](0).map(_.longValue))
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = aggregateArrays(buffer1.getAs[Seq[Long]](0), buffer2.getAs[Seq[Long]](0))
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Array[Long]](0)
  }
}