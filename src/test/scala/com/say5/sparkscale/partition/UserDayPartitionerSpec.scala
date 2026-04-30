package com.say5.sparkscale.partition

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserDayPartitionerSpec extends AnyFlatSpec with Matchers {

  it should "co-locate same (user, day) into same partition" in {
    val p = new UserDayPartitioner(64)
    val a = p.getPartition(("user-42", 19500L))
    val b = p.getPartition(("user-42", 19500L))
    a shouldBe b
  }

  it should "scatter different days for same user" in {
    val p = new UserDayPartitioner(64)
    val partitions = (19500L to 19600L).map { d =>
      p.getPartition(("user-42", d))
    }.toSet
    // 100 days should hit a healthy fraction of the 64 partitions.
    partitions.size should be > 30
  }

  it should "scatter users for same day across many partitions" in {
    val p = new UserDayPartitioner(64)
    val partitions = (1 to 1000).map { i =>
      p.getPartition((s"user-$i", 19500L))
    }.toSet
    // 1000 users on one day should hit most of the 64 partitions.
    partitions.size should be > 50
  }

  it should "produce non-negative partition ids" in {
    val p = new UserDayPartitioner(8)
    (1 to 1000).foreach { i =>
      val part = p.getPartition((s"user-$i", i.toLong))
      part should be >= 0
      part should be < 8
    }
  }

  it should "fall back to userId hashing for plain string keys" in {
    val p = new UserDayPartitioner(16)
    p.getPartition("user-42") should be >= 0
  }

  it should "throw for unsupported keys" in {
    val p = new UserDayPartitioner(16)
    an[IllegalArgumentException] should be thrownBy p.getPartition(42)
  }

  it should "treat partitioners with same numPartitions as equal" in {
    new UserDayPartitioner(64) shouldBe new UserDayPartitioner(64)
    new UserDayPartitioner(64) should not be new UserDayPartitioner(32)
  }
}
