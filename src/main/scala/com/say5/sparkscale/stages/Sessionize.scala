package com.say5.sparkscale.stages

import com.say5.sparkscale.pipeline.Stage
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Sessionize raw clicks into session IDs.
  *
  * A session is a run of clicks for the same user with no gap >
  * `sessionGapMs`. The standard SQL trick: lag-on-prev, mark a
  * gap when current_ts - prev_ts > threshold, then `sum` the gap
  * indicator over the user's ordered events to get a running
  * session id.
  *
  * Output adds a `derivedSessionId` column. The original
  * `sessionId` (if present) is left alone — analytics teams often
  * want both: the cookie-level session vs. the gap-derived
  * session.
  */
object Sessionize {
  def stage(sessionGapMs: Long = 30L * 60L * 1000L): Stage =
    Stage.of("sessionize") { df =>
      val w = Window.partitionBy("userId").orderBy("tsMs")
      val withGap = df
        .withColumn("prevTs", lag(col("tsMs"), 1).over(w))
        .withColumn("isGap",
          when(col("prevTs").isNull || col("tsMs") - col("prevTs") > sessionGapMs, 1)
            .otherwise(0))
        .withColumn("sessionIdx", sum(col("isGap")).over(w))
      withGap.withColumn(
        "derivedSessionId",
        concat_ws("-", col("userId"), col("sessionIdx").cast("string")))
        .drop("prevTs", "isGap", "sessionIdx")
    }
}
