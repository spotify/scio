package com.spotify.scio.examples.extra

import com.spotify.scio.avro.{Account, TestRecord}
import com.spotify.scio.testing._

class AvroInOutTest extends PipelineSpec {

  val input = Seq(
    new TestRecord(1, 0L, 0F, 1000.0, false, "Alice"),
    new TestRecord(2, 0L, 0F, 1500.0, false, "Bob"))

  val expected = Seq(
    new Account(1, "checking", "Alice", 1000.0),
    new Account(2, "checking", "Bob", 1500.0))

  "AvroInOut" should "work" in {
    JobTest("com.spotify.scio.examples.extra.AvroInOut")
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO[TestRecord]("in.avro"), input)
      .output[Account](AvroIO[Account]("out.avro"))(_ should containInAnyOrder (expected))
      .run()
  }

}
