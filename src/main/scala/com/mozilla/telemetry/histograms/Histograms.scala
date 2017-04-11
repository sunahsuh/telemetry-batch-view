package com.mozilla.telemetry.histograms

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.{Try, Success}

class ReduceHistograms(histogramLength: Int) extends UserDefinedAggregateFunction {
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

trait HasLongitudinalHistogramShape {
  val reducer: ReduceHistograms
  val keys: List[Int]

  def filterExp(df: DataFrame): DataFrame
  def prepColumn: Column
}

trait HasNoFilter extends HasLongitudinalHistogramShape {
  override def filterExp(df: DataFrame): DataFrame = {
    df
  }
}

trait HasSumFilter extends HasLongitudinalHistogramShape {
  override def filterExp(df: DataFrame): DataFrame = {
    val sumArray: Seq[Int] => Int = _.sum
    val sumArrayUDF = udf(sumArray)
    df.filter(sumArrayUDF(col("histogram")) > 0)
  }
}

trait EnumHistograms extends HasLongitudinalHistogramShape with HasSumFilter {
  override def prepColumn: Column = col("histogram") // no-op -- these are already in the correct format
}

trait RealHistograms extends HasLongitudinalHistogramShape with HasSumFilter {
  override def prepColumn: Column = {
    col("histogram").getField("values")
  }
}

sealed abstract class HistogramCollection(val name: String, hd: HistogramDefinition, df: DataFrame)
  extends java.io.Serializable with HasLongitudinalHistogramShape {

  def histogramArrayToMap: Column = {
    val points = keys.zipWithIndex.flatMap {
      case (k, i) => List(lit(k), expr(s"histogram_aggregated[$i]"))
    }
    map(points: _*)
  }

  def histogramToMap(r: Row): Row = {
    // if I weren't so lazy a case class might look better here
    val s = r.toSeq
    println(r)
    Row.fromSeq(
      s.take(4)
      ++ s.drop(5)
      ++ Seq(_histogramToMap(r.getAs[Seq[Long]](4))))
  }

  def _histogramToMap(orig: Seq[Long]): Map[Long, Row] = {
    val n = orig.sum
    val z = keys zip orig
    z.map{ case (k, v) => k.asInstanceOf[Long] -> Row((v.asInstanceOf[Double]/n), v) }.toMap
  }

  // the real statistics will be per-histogram
  def fakeStatistics(r: Row): Row = {
    val agg = r.getAs[Map[Long, Row]](6)
    val actual = agg.values.map(v => v.getAs[Long](1))
    val n = actual.sum
    val zipped = agg.keys zip actual
    val totals = zipped.map {case (k, v) => k * v}
    // this is a hilariously bad way to do this -- don't keep
    // val unfurled = zipped.flatMap(x => List.fill(x._2.toInt)(x._1)).toIndexedSeq
    val cumulative_buckets = zipped.scanRight((0L, -1L, 0L)) {
      case ((k, v), (low, high, _)) => (high + 1, low + v, k)
    }
    val median_buckets = cumulative_buckets.filter(x => x._1 <= n/2 && x._2 >= n/2)
    val median = median_buckets.size match {
      case 1 => median_buckets.head._3.toDouble
      case 2 => median_buckets.map(_._3).sum.toDouble / 2
      case _ => null
    }

    val mean = totals.sum.toDouble / n
    val distance = r(1) match {
      case "control" => List()
      case _ => List(Row(
        "control",
        "chi-square distance permutation test",
        scala.util.Random.nextDouble,
        null,
        null,
        null,
        scala.util.Random.nextDouble
      ))
    }
    val summaries = List(
      ("mean", mean),
     // ("median", median),
      ("standard_deviation", scala.math.sqrt(zipped.map(x => scala.math.pow(x._1.toDouble - mean, 2) * x._2).foldLeft(0d)(_ + _)/n))
    )
    val summaryRows = summaries.map {case (name, value) => Row(null, name, value, null, null, null, null)}
    val stats = summaryRows ++ distance
    Row.fromSeq(r.toSeq :+ stats)
  }

  def analyze(): List[Row] = {
    println(name)

    val exploded: DataFrame = Try {
      df.select(
        col("experiment").as("experiment_name"),
        col("branch").as("experiment_branch"),
        lit(null).as("subgroup"),
        explode(col(name)).as("histogram")
      ).persist()
    } match {
      case Success(x) => x
      case _ => return List() // fails with an unhelpful exception when explode has no results
    }

    // return if empty after exploding, will die otherwise
    if (exploded.take(1).length == 0) return List()
    val subset = filterExp(exploded.withColumn("histogram", prepColumn))
    val aggregate = subset
      .groupBy(col("experiment_name"), col("experiment_branch"), col("subgroup"))
      .agg(count("histogram").as("n"), reducer(col("histogram")).as("histogram_agg"))
      .withColumn("metric_name", lit(name))
      .withColumn("metric_type", lit(hd.getClass.getSimpleName))
      // .withColumn("histogram", histogramArrayToMap)
    aggregate.show()
    exploded.unpersist()
    val row = aggregate.rdd.map(histogramToMap).map(fakeStatistics).collect.toList
    row.foreach(println)
    row
  }
}

object HistogramCollection {
  def createCollection(name: String, hd: HistogramDefinition, df: DataFrame): HistogramCollection = {
    hd match {
      case t: FlagHistogram => new FlagHistogramCollection(name, t, df)
      case t: BooleanHistogram => new BooleanHistogramCollection(name, t, df)
      case t: CountHistogram => new CountHistogramCollection(name, t, df)
      case t: EnumeratedHistogram => new EnumeratedHistogramCollection(name, t, df)
      case t: LinearHistogram => new LinearHistogramCollection(name, t, df)
      case t: ExponentialHistogram => new ExponentialHistogramCollection(name, t, df)
      case t => throw new Exception("Unknown histogram definition type" + t.getClass.getSimpleName)
    }
  }
}

class FlagHistogramCollection(name: String, hd: FlagHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df)
    with HasNoFilter {
  val reducer = new ReduceHistograms(2)
  val keys = List(0, 1)

  override def prepColumn: Column = {
    expr("if(histogram == true, Array(0, 1), Array(1, 0))")
  }
}

class BooleanHistogramCollection(name: String, hd: BooleanHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df)
    with EnumHistograms {
  val reducer = new ReduceHistograms(2)
  val keys = List(0, 1)
}

class CountHistogramCollection(name: String, hd: CountHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df) with HasSumFilter {
  val reducer = new ReduceHistograms(1)
  val keys = List(0)

  override def prepColumn: Column = array("histogram")
}

class EnumeratedHistogramCollection(name: String, hd: EnumeratedHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df)
    with EnumHistograms {
  val reducer = new ReduceHistograms(hd.nValues)
  val keys = (0 until hd.nValues).toList
}

class LinearHistogramCollection(name: String, hd: LinearHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df)
    with RealHistograms {
  val reducer = new ReduceHistograms(hd.nBuckets)
  val keys = Histograms.linearBuckets(hd.low, hd.high, hd.nBuckets).toList
}

class ExponentialHistogramCollection(name: String, hd: ExponentialHistogram, df: DataFrame)
  extends HistogramCollection(name, hd, df)
    with RealHistograms {
  val reducer = new ReduceHistograms(hd.nBuckets)
  val keys = Histograms.exponentialBuckets(hd.low, hd.high, hd.nBuckets).toList
}
