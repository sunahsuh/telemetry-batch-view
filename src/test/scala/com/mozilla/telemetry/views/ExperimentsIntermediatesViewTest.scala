package com.mozilla.telemetry.views

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

case class ExperimentSummaryRow(
  client_id: String,
  experiment_id: String,
  experiment_branch: String,
  scalar_content_browser_usage_graphite: Int,
  histogram_content_gc_max_pause_ms: Map[Int, Int])

// It appears a case class has to be accessible in the scope the spark session
// is created in or implicit conversions won't work
case class PermutationsRow(client_id: String)

class ExperimentIntermediatesViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession =
    SparkSession.builder()
    .appName("Experiment Aggregate Test")
    .master("local[*]")
    .getOrCreate()

  val predata = Seq(
    ExperimentSummaryRow("a", "id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("b", "id2", "branch1", 2, Map(2 -> 1)),
    ExperimentSummaryRow("c", "id2", "control", 1, Map(2 -> 1)),
    ExperimentSummaryRow("d", "id2", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow("e", "id3", "control", 1, Map(2 -> 1))
  )

  "Child Scalars" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF().where(col("experiment_id") === "id1")
    val args =
      "--bucket" :: "telemetry-mock-bucket" ::
      "--inbucket" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentsIntermediatesView.Conf(args.toArray)

    try {
      val res = ExperimentsIntermediatesView.getAggregates("id1", data, conf)
      val agg = res.filter(_.metric_name == "scalar_content_browser_usage_graphite").head
      agg.histogram(1).pdf should be(1.0)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  "Child Histograms" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF().where(col("experiment_id") === "id1")
    val args =
      "--bucket" :: "telemetry-mock-bucket" ::
      "--inbucket" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentsIntermediatesView.Conf(args.toArray)

    val res = ExperimentsIntermediatesView.getAggregates("id1", data, conf)
    val agg = res.filter(_.metric_name == "histogram_content_gc_max_pause_ms").head
    agg.histogram(1).pdf should be (1.0)
  }

  "Permutations column" can "be added" in {
    import spark.implicits._

    val data = Seq(
      PermutationsRow("a"),
      PermutationsRow("a"),
      PermutationsRow("b"),
      PermutationsRow("c"),
      PermutationsRow("d")
    )
    val res = ExperimentsIntermediatesView
      .addPermutations(data.toDS.toDF, Map("control" -> 2, "branch1" -> 1, "branch2" -> 2), "test_experiment")
      .select("permutations")
      .collect()
    val counts = res
      .flatMap(_.getAs[Seq[Byte]](0))
      .groupBy(identity)
      .mapValues(_.size)

    // 0 should map to "branch1", first alphabetically
    counts(0x00) should be (100 +- 15)
    // 1 should map to "branch2", second alphabetically
    counts(0x01) should be (200 +- 30)
    // 2 should map to "control", last alphabetically
    counts(0x02) should be (200 +- 30)

    // the permutations for client_id "a" should be the same
    res(0).getAs[Seq[Byte]](0) should be (res(1).getAs[Seq[Byte]](0))
  }

  override def afterAll() {
    spark.stop()
  }
}
