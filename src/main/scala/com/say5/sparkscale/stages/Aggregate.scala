package com.say5.sparkscale.stages

import com.say5.sparkscale.pipeline.Stage
import org.apache.spark.sql.functions._

/** Daily-rollup aggregations the analytics team needs:
  *
  *  - clicks_per_user_per_day
  *  - sessions_per_user_per_day (uses derivedSessionId from
  *    [[Sessionize]])
  *  - distinct_pages_per_user_per_day
  *
  * Output schema: (userId, ts_day, clicks, sessions,
  * distinct_pages). Joins back onto user-dimension tables in the
  * downstream warehouse.
  */
object Aggregate {

  def daily: Stage = Stage.of("aggregate-daily") { df =>
    df.withColumn("ts_day", floor(col("tsMs") / 86_400_000L))
      .groupBy("userId", "ts_day")
      .agg(
        count(lit(1)).as("clicks"),
        countDistinct(col("derivedSessionId")).as("sessions"),
        countDistinct(col("pageUrl")).as("distinct_pages"),
      )
  }
}
