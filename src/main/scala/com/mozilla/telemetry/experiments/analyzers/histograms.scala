package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.histograms._
import com.mozilla.telemetry.utils.userdefinedfunctions.AggregateHistograms
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

trait LongitudinalHistogramAnalyzer {
  val reducer: AggregateHistograms
  val keys: List[Int]

  val keyedUDF: UserDefinedFunction
  def filterExp(df: DataFrame): DataFrame
  def nonKeyedUDF: UserDefinedFunction
}

sealed abstract class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame) extends MetricAnalyzer(name, hd, df)
  with LongitudinalHistogramAnalyzer {
  def histogramToMap(r: Row): Row = {
    // A case class might look better here
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
    z.map{ case (k, v) => k.asInstanceOf[Long] -> Row(v.asInstanceOf[Double] / n, v, null) }.toMap
  }

  // the real statistics will be per-histogram
  def createStatistics(r: Row): Row = {
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

  def handleKeys: Column = if (hd.keyed) keyedUDF(col("metric")) else nonKeyedUDF(col("metric"))

  def analyze(): List[Row] = {
    println(name)

    val filtered: DataFrame =
      Try(df.select(
        col("experiment").as("experiment_name"),
        col("branch").as("experiment_branch"),
        lit(null).as("subgroup"),
        col(name).as("metric"))
        .filter(col("metric").isNotNull)
        .persist() // We want to persist here because this df is what we'll want to run our permutation tests on
      ) match {
        case Success(x) => x
        // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
        case Failure(_: org.apache.spark.sql.AnalysisException) => return List()
        // Let other exceptions bubble up and kill the job
      }
    val exploded: DataFrame =
      filtered.select(
        col("experiment_name"),
        col("experiment_branch"),
        col("subgroup"),
        explode(handleKeys).as("metric"))
        .filter(col("metric").isNotNull) // Second null filter to find individual nulls
    val aggregate = exploded
      .groupBy(col("experiment_name"), col("experiment_branch"), col("subgroup"))
      .agg(count("metric").as("n"), reducer(col("metric")).as("histogram_agg"))
      .withColumn("metric_name", lit(name))
      .withColumn("metric_type", lit(hd.getClass.getSimpleName))
      .coalesce(1)
      .persist() // persist this one so we can run summary stats
    aggregate.show()
    val rows = aggregate.rdd.map(histogramToMap).map(createStatistics).collect.toList
    rows.foreach(println)
    filtered.unpersist()
    aggregate.unpersist()
    rows
  }
}

trait HasNoFilter extends LongitudinalHistogramAnalyzer {
  override def filterExp(df: DataFrame): DataFrame = df
}

trait HasSumFilter extends LongitudinalHistogramAnalyzer {
  override def filterExp(df: DataFrame): DataFrame = {
    val sumArray: Seq[Int] => Int = _.sum
    val sumArrayUDF = udf(sumArray)
    df.filter(sumArrayUDF(col("metric")) > 0)
  }
}

trait EnumHistograms extends LongitudinalHistogramAnalyzer with HasSumFilter {
  def collapseKeys(m: Map[String, Seq[Seq[Int]]]): Seq[Seq[Int]] = {
    m match {
      case null => null
      case _ => {
        // h: array of each key's histogram array
        // _: array of each key's values for a bucket
        m.values.transpose.map(h => h.transpose.map(_.sum).toSeq).toSeq
      }
    }
  }

  lazy val keyedUDF = udf(collapseKeys _)
  lazy val nonKeyedUDF = udf(prepColumn _)

  // No transformation necessary
  def prepColumn(m: Seq[Seq[Int]]): Seq[Seq[Int]] = m
}

trait RealHistograms extends LongitudinalHistogramAnalyzer with HasSumFilter {
  def collapseKeys(m: Map[String, Seq[Row]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(
        h => h.map(r => r.getAs[Seq[Int]]("values")).transpose.map(_.sum).toSeq
      ).toSeq
    }
  }

  lazy val keyedUDF = udf(collapseKeys _)
  lazy val nonKeyedUDF = udf(prepColumn _)

  def prepColumn(m: Seq[Row]): Seq[Seq[Int]] = m.map(_.getSeq[Int](0))
}

class FlagHistogramAnalyzer(name: String, hd: FlagHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with HasNoFilter {
  val reducer = new AggregateHistograms(2)
  val keys = List(0, 1)

  def collapseKeys(m: Map[String, Seq[Boolean]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(h => h.map(_ match {
        case true => Array(0, 1)
        case false => Array(1, 0)
      }).transpose.map(_.sum).toSeq).toSeq
    }
  }

  lazy val keyedUDF = udf(collapseKeys _)
  lazy val nonKeyedUDF = udf(prepColumn _)

  def prepColumn(m: Seq[Boolean]): Seq[Seq[Int]] = m.map(if (_) Seq(0, 1) else Seq(1, 0))
}

class BooleanHistogramAnalyzer(name: String, hd: BooleanHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with EnumHistograms {
  val reducer = new AggregateHistograms(2)
  val keys = List(0, 1)
}

class CountHistogramAnalyzer(name: String, hd: CountHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) with HasSumFilter {
  val reducer = new AggregateHistograms(1)
  val keys = List(0)

  def collapseKeys(m: Map[String, Seq[Int]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(x => Seq(x.sum)).toSeq
    }
  }
  lazy val keyedUDF = udf(collapseKeys _)
  lazy val nonKeyedUDF = udf(prepColumn _)

  def prepColumn(m: Seq[Int]): Seq[Seq[Int]] = m.map(Seq(_))
}

class EnumeratedHistogramAnalyzer(name: String, hd: EnumeratedHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with EnumHistograms {
  val reducer = new AggregateHistograms(hd.nValues)
  val keys = (0 until hd.nValues).toList
}

class LinearHistogramAnalyzer(name: String, hd: LinearHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with RealHistograms {
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys = Histograms.linearBuckets(hd.low, hd.high, hd.nBuckets).toList
}

class ExponentialHistogramAnalyzer(name: String, hd: ExponentialHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with RealHistograms {
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys = Histograms.exponentialBuckets(hd.low, hd.high, hd.nBuckets).toList
}