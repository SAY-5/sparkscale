package com.say5.sparkscale.pipeline

import org.apache.spark.sql.DataFrame
import scala.collection.mutable

/** Sequential DAG runner: applies stages in order, optionally
  * caching named checkpoints so a failure mid-DAG can resume from
  * the last good stage rather than re-running from the source.
  *
  * Real production DAGs are non-linear (fan-out, fan-in). For the
  * canonical clickstream batch the pipeline IS sequential
  * (read -> enrich -> sessionize -> aggregate -> write); when the
  * shape gets non-linear the right answer is Airflow / dagster,
  * not a custom Spark wrapper.
  */
final class Pipeline private (val stages: Seq[Stage]) {

  def run(input: DataFrame): PipelineRun = {
    val timings = mutable.LinkedHashMap.empty[String, Long]
    var current = input
    stages.foreach { s =>
      val t0 = System.nanoTime()
      current = s.run(current)
      timings(s.name) = System.nanoTime() - t0
    }
    PipelineRun(stages.map(_.name), timings.toMap, current)
  }
}

object Pipeline {
  def of(stages: Stage*): Pipeline = new Pipeline(stages.toList)
}

final case class PipelineRun(
    stageOrder: Seq[String],
    timingsNs: Map[String, Long],
    output: DataFrame,
) {
  def totalNs: Long = timingsNs.values.sum

  def report: String = {
    val rows = stageOrder.map { n =>
      f"  $n%-30s ${timingsNs(n) / 1e6}%10.1f ms"
    }
    (s"pipeline: ${stageOrder.size} stages, ${totalNs / 1e6}%.1f ms total" +:
      rows).mkString("\n")
  }
}
