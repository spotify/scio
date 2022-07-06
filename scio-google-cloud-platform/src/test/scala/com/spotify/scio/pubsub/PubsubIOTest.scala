/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.pubsub

import com.spotify.scio.testing.PipelineSpec
import com.google.protobuf.Message
import org.apache.beam.sdk.io.gcp.{pubsub => beam}
import com.spotify.scio._
import com.spotify.scio.avro.{Account, AccountStatus}
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing._
import com.spotify.scio.pubsub.coders._
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.joda.time.Instant

class PubsubIOTest extends PipelineSpec with ScioIOSpec {
  "PubsubIO" should "work with subscription" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubsubIO(_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  it should "work with topic" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubsubIO(_))(_.pubsubTopic(_))(_.saveAsPubsub(_))
  }

  it should "work with subscription and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    val io = (s: String) => PubsubIO[(String, Map[String, String])](s)
    testJobTest(xs)(io)(_.pubsubSubscriptionWithAttributes(_))(
      _.saveAsPubsubWithAttributes[String](_)
    )
  }

  it should "work with topic and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    val io = (s: String) => PubsubIO[(String, Map[String, String])](s)
    testJobTest(xs)(io)(_.pubsubTopicWithAttributes(_))(_.saveAsPubsubWithAttributes[String](_))
  }

  it should "#2582: not throw an ClassCastException when created" in {
    PubsubIO[String]("String IO")
    PubsubIO[com.spotify.scio.avro.Account]("SpecificRecordBase IO")
    PubsubIO[Message]("Message IO")
    PubsubIO[beam.PubsubMessage]("Message IO")
  }

  it should "support deprecated readAvro" in {
    val xs = (1 to 100).map(x => new Account(x, "", "", x.toDouble, AccountStatus.Active))
    testJobTest(xs)(PubsubIO.readAvro[Account](_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  it should "support deprecated readString" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubsubIO.readString(_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  it should "support deprecated readProto" in {
    val xs = (1 to 100).map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
    testJobTest(xs)(PubsubIO.readProto(_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  it should "support deprecated readCoder" in {
    val xs = 1 to 100
    testJobTest(xs)(PubsubIO.readCoder(_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  def testPubsubJob(xs: String*): Unit =
    JobTest[PubsubJob.type]
      .args("--input=in", "--output=out")
      .input(PubsubIO[String]("in"), Seq("a", "b", "c"))
      .output(PubsubIO[String]("out"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct PubsubIO" in {
    testPubsubJob("aX", "bX", "cX")
  }

  it should "fail incorrect PubsubIO" in {
    an[AssertionError] should be thrownBy { testPubsubJob("aX", "bX") }
    an[AssertionError] should be thrownBy {
      testPubsubJob("aX", "bX", "cX", "dX")
    }
  }

  def testPubsubJobWithTestStreamInput(xs: String*): Unit =
    JobTest[PubsubJob.type]
      .args("--input=in", "--output=out")
      .inputStream(
        PubsubIO[String]("in"),
        testStreamOf[String].addElements("a", "b", "c").advanceWatermarkToInfinity()
      )
      .output(PubsubIO[String]("out"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct PubsubIO with TestStream input" in {
    testPubsubJobWithTestStreamInput("aX", "bX", "cX")
  }

  it should "fail incorrect PubsubIO with TestStream input" in {
    an[AssertionError] should be thrownBy testPubsubJobWithTestStreamInput("aX", "bX")
    an[AssertionError] should be thrownBy testPubsubJobWithTestStreamInput("aX", "bX", "cX", "dX")
  }

  def testPubsubWithAttributesJob(timestampAttribute: Map[String, String], xs: String*): Unit = {
    type M = Map[String, String]
    val m = Map("a" -> "1", "b" -> "2", "c" -> "3") ++ timestampAttribute
    JobTest[PubsubWithAttributesJob.type]
      .args("--input=in", "--output=out")
      .input(
        PubsubIO[(String, M)]("in", null, PubsubWithAttributesJob.timestampAttribute),
        Seq("a", "b", "c").map((_, m))
      )
      .output(
        PubsubIO[(String, M)]("out", null, PubsubWithAttributesJob.timestampAttribute)
      )(coll => coll should containInAnyOrder(xs.map((_, m))))
      .run()
  }

  it should "pass correct PubsubIO with attributes" in {
    testPubsubWithAttributesJob(
      Map(PubsubWithAttributesJob.timestampAttribute -> new Instant().toString),
      "aX",
      "bX",
      "cX"
    )
  }

  it should "fail incorrect PubsubIO with attributes" in {
    an[AssertionError] should be thrownBy {
      testPubsubWithAttributesJob(
        Map(PubsubWithAttributesJob.timestampAttribute -> new Instant().toString),
        "aX",
        "bX"
      )
    }
    an[AssertionError] should be thrownBy {
      testPubsubWithAttributesJob(
        Map(PubsubWithAttributesJob.timestampAttribute -> new Instant().toString),
        "aX",
        "bX",
        "cX",
        "dX"
      )
    }
  }

  it should "fail PubsubIO with invalid or missing timestamp attribute" in {
    an[PipelineExecutionException] should be thrownBy {
      testPubsubWithAttributesJob(
        Map(PubsubWithAttributesJob.timestampAttribute -> "invalidTimestamp"),
        "aX",
        "bX",
        "cX"
      )
    }

    an[PipelineExecutionException] should be thrownBy {
      testPubsubWithAttributesJob(timestampAttribute = Map(), xs = "aX", "bX", "cX")
    }
  }

}

object PubsubJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.pubsubTopic[String](args("input"))
      .map(_ + "X")
      .saveAsPubsub(args("output"))
    sc.run()
    ()
  }
}

object PubsubWithAttributesJob {
  val timestampAttribute = "tsAttribute"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.pubsubTopicWithAttributes[String](args("input"), timestampAttribute = timestampAttribute)
      .map(kv => (kv._1 + "X", kv._2))
      .saveAsPubsubWithAttributes[String](args("output"), timestampAttribute = timestampAttribute)
    sc.run()
    ()
  }
}
