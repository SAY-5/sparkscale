package com.say5.sparkscale.event

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

/** One row in the daily clickstream feed.
  *
  * Field choice is the canonical-minimum: who clicked, on what,
  * when, from where. Production schemas carry 30-50 fields per
  * event (browser, OS, IP-derived geo, A/B segment); this models
  * the core five so the pipeline shape is readable.
  *
  * `tsMs` is epoch millis, not a Spark `Timestamp`, because we
  * partition on `ts_day = floor(tsMs / 86400_000)` and integer
  * division is faster than timestamp casting in the hot path.
  */
final case class ClickEvent(
    userId: String,
    sessionId: String,
    pageUrl: String,
    elementId: String,
    tsMs: Long,
)

object ClickEvent {
  implicit val encoder: Encoder[ClickEvent] = Encoders.product[ClickEvent]

  /** Day bucket used by the custom partitioner.
    * 86_400_000 = ms in a day. */
  def dayOf(tsMs: Long): Long = tsMs / 86_400_000L
}
