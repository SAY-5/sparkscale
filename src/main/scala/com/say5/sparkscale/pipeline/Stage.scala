package com.say5.sparkscale.pipeline

import org.apache.spark.sql.DataFrame

/** One named transformation step in the pipeline DAG. The shape
  * is intentionally minimal so users plug in their own logic
  * without inheriting a heavyweight base class.
  */
trait Stage {
  def name: String
  def run(input: DataFrame): DataFrame
}

object Stage {
  def of(stageName: String)(fn: DataFrame => DataFrame): Stage =
    new Stage {
      override val name: String = stageName
      override def run(input: DataFrame): DataFrame = fn(input)
    }
}
