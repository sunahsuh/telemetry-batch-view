package com.mozilla.telemetry.utils

import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.mozilla.telemetry.histograms._
import com.mozilla.telemetry.scalars._
import com.mozilla.telemetry.utils.userdefinedfunctions.{AggregateHistograms, AggregateScalars}

import scala.util.{Success, Try, Failure}

sealed abstract class MetricAnalyzer(name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  val reducer: UserDefinedAggregateFunction
  def handleKeys: Column
  val keyedUDF: UserDefinedFunction

  def analyze: List[Row]

  // We want to keep this output since we'll need this to do the experimental distance metrics
  // Question: for permutation tests, are the null values significant? e.g. should we be doing the tests on the DF before
  // filtering out nulls?
  def formatAndFilter: Option[DataFrame] = {
    Try(df.select(
      col("experiment").as("experiment_name"),
      col("branch").as("experiment_branch"),
      lit(null).as("subgroup"),
      col(name).as("metric"))
      .filter(col("metric").isNotNull)
    ) match {
      case Success(x) => Some(x)
      // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
      case Failure(_: org.apache.spark.sql.AnalysisException) => None
      // Let other exceptions bubble up
    }
  }
  def explodeMetric(filtered: DataFrame): DataFrame = {
      filtered.select(
        col("experiment_name"),
        col("experiment_branch"),
        col("subgroup"),
        explode(handleKeys).as("metric"))
        .filter(col("metric").isNotNull)
  }

  def aggregate(exploded: DataFrame): DataFrame = {
    val aggregate = exploded
      .groupBy(col("experiment_name"), col("experiment_branch"), col("subgroup"))
      .agg(count("metric").as("n"), reducer(col("metric")).as("histogram_agg"))
      .withColumn("metric_name", lit(name))
      .withColumn("metric_type", lit(md.getClass.getSimpleName))
      .coalesce(1)
      .persist() // persist this one so we can run summary stats
    aggregate.show()
    aggregate
  }

  def runSummaryStatistics(aggregates: DataFrame): DataFrame = {
    // TODO: fill me in!
    aggregates
  }

  def runTestStatistics(filtered: DataFrame, aggregates: DataFrame): DataFrame = {
    // TODO: fill me in!
    aggregates
  }
}

sealed abstract class ScalarAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame) extends MetricAnalyzer(name, sd, df) {
  def handleKeys: Column = if (sd.keyed) keyedUDF(col("metric")) else col(s"$name.value")
}

object ScalarAnalyzer {
  def getAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame): ScalarAnalyzer = {
    sd match {
      case t: UintScalar => new UintScalarAnalyzer(name, t, df)
      case t: StringScalar => new StringScalarAnalyzer(name, t, df)
      case t => throw new Exception("Unknown histogram definition type " + t.getClass.getSimpleName)
    }
  }
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
    val rows = runTestStatistics(filtered, withSummaries).collect().toList
    // TODO: test if unpersist is lazy -- if not, this is meaningless
    filtered.unpersist()
    aggregates.unpersist()
    rows
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
    val aggregates = aggregate(explodeMetric(filtered))
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

trait LongitudinalHistogramAnalyzer {
  // fix inheritence to make this unnecessary
  val isKeyed: Boolean
  val reducer: AggregateHistograms
  val keys: List[Int]

  val keyedUDF: UserDefinedFunction
  def filterExp(df: DataFrame): DataFrame
  def prepColumn: Column
}

sealed abstract class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame) extends MetricAnalyzer(name, hd, df)
with LongitudinalHistogramAnalyzer {
  val isKeyed = hd.keyed

  def histogramToMap(r: Row): Row = {
    // if I weren't lazy a case class might look better here
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
    z.map{ case (k, v) => k.asInstanceOf[Long] -> Row(v.asInstanceOf[Double] / n, v) }.toMap
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

  def handleKeys: Column = if (hd.keyed) keyedUDF(col("metric")) else col("metric")

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
    val subset = filterExp(exploded.withColumn("metric", prepColumn))
    val aggregate = subset
      .groupBy(col("experiment_name"), col("experiment_branch"), col("subgroup"))
      .agg(count("metric").as("n"), reducer(col("metric")).as("histogram_agg"))
      .withColumn("metric_name", lit(name))
      .withColumn("metric_type", lit(hd.getClass.getSimpleName))
      .coalesce(1)
      .persist() // persist this one so we can run summary stats
    aggregate.show()
    exploded.unpersist()
    val rows = aggregate.rdd.map(histogramToMap).map(createStatistics).collect.toList
    rows.foreach(println)
    rows
  }
}

object HistogramAnalyzer {
  def getAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame): HistogramAnalyzer = {
    hd match {
      case t: FlagHistogram => new FlagHistogramAnalyzer(name, t, df)
      case t: BooleanHistogram => new BooleanHistogramAnalyzer(name, t, df)
      case t: CountHistogram => new CountHistogramAnalyzer(name, t, df)
      case t: EnumeratedHistogram => new EnumeratedHistogramAnalyzer(name, t, df)
      case t: LinearHistogram => new LinearHistogramAnalyzer(name, t, df)
      case t: ExponentialHistogram => new ExponentialHistogramAnalyzer(name, t, df)
      case t => throw new Exception("Unknown histogram definition type" + t.getClass.getSimpleName)
    }
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

  override def prepColumn: Column = col("metric") // no-op -- these are already in the correct format
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

  override def prepColumn: Column = {
    isKeyed match {
      case false => col("metric").getField("values")
      case _ => col("metric")
    }
  }
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

  override def prepColumn: Column = hd.keyed match {
    case false => expr("if(metric == true, Array(0, 1), Array(1, 0))")
    case true => col("metric")
  }
}

class BooleanHistogramAnalyzer(name: String, hd: BooleanHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with EnumHistograms {
  val keyed = hd.keyed
  val reducer = new AggregateHistograms(2)
  val keys = List(0, 1)
}

class CountHistogramAnalyzer(name: String, hd: CountHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) with HasSumFilter {
  val keyed = hd.keyed
  val reducer = new AggregateHistograms(1)
  val keys = List(0)

  def collapseKeys(m: Map[String, Seq[Int]]): Seq[Int] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(_.sum).toSeq
    }
  }
  lazy val keyedUDF = udf(collapseKeys _)

  override def prepColumn: Column = array("metric")
}

class EnumeratedHistogramAnalyzer(name: String, hd: EnumeratedHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with EnumHistograms {
  val keyed = hd.keyed
  val reducer = new AggregateHistograms(hd.nValues)
  val keys = (0 until hd.nValues).toList
}

class LinearHistogramAnalyzer(name: String, hd: LinearHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with RealHistograms {
  val keyed = hd.keyed
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys = Histograms.linearBuckets(hd.low, hd.high, hd.nBuckets).toList
}

class ExponentialHistogramAnalyzer(name: String, hd: ExponentialHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df)
    with RealHistograms {
  val keyed = hd.keyed
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys = Histograms.exponentialBuckets(hd.low, hd.high, hd.nBuckets).toList
}


