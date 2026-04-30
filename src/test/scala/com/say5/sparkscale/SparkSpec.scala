package com.say5.sparkscale

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

/** Shared SparkSession for tests. We reuse one session across all
  * specs to avoid the ~3 second startup cost per file. */
trait SparkSpec extends AnyFlatSpec with BeforeAndAfterAll {

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("sparkscale-test")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  override def afterAll(): Unit = {
    super.afterAll()
    // Don't stop — JVM-wide singleton; sbt forks a new VM per
    // test class anyway when fork is enabled.
  }
}
