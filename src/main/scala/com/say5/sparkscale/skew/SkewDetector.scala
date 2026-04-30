package com.say5.sparkscale.skew

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** v4: detect partition skew before it kills a stage.
  *
  * Skew = a small number of partitions (often one) carrying a
  * disproportionate share of the rows. In Spark, the slowest
  * partition gates the stage's wall-clock — so 99 fast partitions
  * + 1 slow one means the whole stage runs at the slow one's
  * speed.
  *
  * Detection: bucket rows by the candidate partition key, count
  * per bucket, flag any bucket whose count is more than
  * `skewFactor` times the median. Default factor = 3.
  *
  * Output: a SkewReport. The orchestrator can branch on this:
  * skew detected → switch to a salting strategy (append a small
  * random suffix to the key, then strip it after the join);
  * no skew → run the natural plan.
  */
final case class SkewReport(
    keyColumn: String,
    medianCount: Long,
    maxCount: Long,
    skewFactor: Double,
    skewedKeys: Seq[(Any, Long)],
) {
  def hasSkew: Boolean = skewedKeys.nonEmpty
}

object SkewDetector {

  def detect(df: DataFrame, keyColumn: String, factor: Double = 3.0): SkewReport = {
    val counts = df.groupBy(col(keyColumn).as("k"))
      .count()
      .as("c")
      .collect()
      .map(r => (r.get(0), r.getLong(1)))

    if (counts.isEmpty) {
      return SkewReport(keyColumn, 0L, 0L, 0.0, Seq.empty)
    }

    val sorted = counts.map(_._2).sorted
    val median = sorted(sorted.length / 2)
    val max = sorted.last
    val threshold = (median * factor).toLong
    val skewed = counts.filter { case (_, c) => c > threshold }
      .sortBy { case (_, c) => -c }
      .toSeq

    SkewReport(
      keyColumn = keyColumn,
      medianCount = median,
      maxCount = max,
      skewFactor = if (median == 0) 0.0 else max.toDouble / median.toDouble,
      skewedKeys = skewed,
    )
  }
}
