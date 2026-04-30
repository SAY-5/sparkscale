package com.say5.sparkscale.io

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetWriterSpec extends AnyFlatSpec with Matchers {

  it should "suggest 1 partition for tiny inputs" in {
    ParquetWriter.suggestedPartitions(0L) shouldBe 1
    ParquetWriter.suggestedPartitions(1024L) shouldBe 1
  }

  it should "scale partitions with input size" in {
    val gb = 1024L * 1024L * 1024L
    // 1 GB / 128 MB target = 8 partitions
    ParquetWriter.suggestedPartitions(gb) shouldBe 8
    // 10 GB / 128 MB = 80 partitions
    ParquetWriter.suggestedPartitions(10 * gb) shouldBe 80
  }

  it should "always return at least 1 partition" in {
    ParquetWriter.suggestedPartitions(-1L) shouldBe 1
  }
}
