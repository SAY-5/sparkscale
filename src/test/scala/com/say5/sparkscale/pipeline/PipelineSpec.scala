package com.say5.sparkscale.pipeline

import com.say5.sparkscale.SparkSpec
import com.say5.sparkscale.event.ClickEvent
import com.say5.sparkscale.stages.{Aggregate, Sessionize}
import org.scalatest.matchers.should.Matchers

class PipelineSpec extends SparkSpec with Matchers {

  it should "run sessionize then aggregate end to end" in {
    import spark.implicits._

    val now = 1700000000000L
    val events = Seq(
      // user A: 3 clicks in one session
      ClickEvent("a", "s1", "/home", "btn1", now),
      ClickEvent("a", "s1", "/list", "btn2", now + 5_000),
      ClickEvent("a", "s1", "/item", "btn3", now + 10_000),
      // user A: a 2nd session 1 hour later
      ClickEvent("a", "s2", "/home", "btn1", now + 3600_000),
      // user B: 2 clicks
      ClickEvent("b", "s3", "/home", "btn1", now),
      ClickEvent("b", "s3", "/cart", "btn4", now + 2_000),
    ).toDS().toDF()

    val pipeline = Pipeline.of(
      Sessionize.stage(),
      Aggregate.daily,
    )
    val run = pipeline.run(events)
    val rows = run.output.collect()

    rows.length shouldBe 2  // user A + user B (one day)
    val byUser = rows.map(r => r.getAs[String]("userId") -> r).toMap
    byUser("a").getAs[Long]("clicks") shouldBe 4
    byUser("a").getAs[Long]("sessions") shouldBe 2
    byUser("b").getAs[Long]("clicks") shouldBe 2
    byUser("b").getAs[Long]("sessions") shouldBe 1
  }

  it should "capture per-stage timings" in {
    import spark.implicits._
    val df = Seq(ClickEvent("a", "s", "/x", "y", 1L)).toDS().toDF()
    val run = Pipeline.of(Sessionize.stage(), Aggregate.daily).run(df)
    run.timingsNs.keySet shouldBe Set("sessionize", "aggregate-daily")
    run.totalNs should be > 0L
  }

  it should "report all stage names in declaration order" in {
    import spark.implicits._
    val df = Seq(ClickEvent("a", "s", "/x", "y", 1L)).toDS().toDF()
    val run = Pipeline.of(Sessionize.stage(), Aggregate.daily).run(df)
    run.stageOrder shouldBe Seq("sessionize", "aggregate-daily")
  }
}
