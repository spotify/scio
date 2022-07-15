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
import com.spotify.scio._
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubJsonClient, PubsubOptions}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.Instant
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient

class PubsubIOTest extends PipelineSpec with ScioIOSpec {
  def readFn[T](
    fn: String => PubsubIO[T],
    readType: PubsubIO.ReadType
  ): (ScioContext, String) => SCollection[T] =
    (sc, in) => sc.read(fn(in))(PubsubIO.ReadParam(readType))

  def writeFn[T](
    fn: String => PubsubIO[T],
    param: PubsubIO.WriteParam = PubsubIO.WriteParam()
  ): (SCollection[T], String) => ClosedTap[_] =
    (sc, out) => sc.write(fn(out))(param)

  val saveAsPubSub: (SCollection[String], String) => ClosedTap[_] = writeFn(PubsubIO.string(_))

  val saveAsPubSubWithAttributes
    : (SCollection[(String, Map[String, String])], String) => ClosedTap[_] =
    writeFn(PubsubIO.withAttributes[String](_))

  val saveAsPubsubWithOptions
    : (SCollection[(String, Map[String, String])], String) => ClosedTap[_] = {
    val io = (s: String) => PubsubIO.withAttributes[String](s)
    val param = PubsubIO.WriteParam(clientFn = PubsubJsonClient.FACTORY.newClient)
    writeFn(io, param)
  }

  "PubsubIO" should "work with subscription" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubsubIO.string(_))(readFn(PubsubIO.string(_), PubsubIO.Subscription))(
      saveAsPubSub
    )
  }

  it should "work with topic" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubsubIO.string(_))(readFn(PubsubIO.string(_), PubsubIO.Topic))(saveAsPubSub)
  }

  it should "work with subscription and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    testJobTest(xs)(PubsubIO.withAttributes[String](_))(
      readFn(PubsubIO.withAttributes(_), PubsubIO.Subscription)
    )(saveAsPubSubWithAttributes)
  }

  it should "work with topic and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    testJobTest(xs)(PubsubIO.withAttributes[String](_))(
      readFn(PubsubIO.withAttributes(_), PubsubIO.Topic)
    )(saveAsPubSubWithAttributes)
  }

  def testPubsubJob(xs: String*): Unit =
    JobTest[PubsubJob.type]
      .args("--input=in", "--output=out")
      .input(PubsubIO.string("in"), Seq("a", "b", "c"))
      .output(PubsubIO.string("out"))(coll => coll should containInAnyOrder(xs))
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
        PubsubIO.string("in"),
        testStreamOf[String].addElements("a", "b", "c").advanceWatermarkToInfinity()
      )
      .output(PubsubIO.string("out"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct PubsubIO with TestStream input" in {
    testPubsubJobWithTestStreamInput("aX", "bX", "cX")
  }

  it should "fail incorrect PubsubIO with TestStream input" in {
    an[AssertionError] should be thrownBy testPubsubJobWithTestStreamInput("aX", "bX")
    an[AssertionError] should be thrownBy testPubsubJobWithTestStreamInput("aX", "bX", "cX", "dX")
  }

  def testPubsubWithAttributesJob(timestampAttribute: Map[String, String], xs: String*): Unit = {
    val m = Map("a" -> "1", "b" -> "2", "c" -> "3") ++ timestampAttribute
    JobTest[PubsubWithAttributesJob.type]
      .args("--input=in", "--output=out")
      .input(
        PubsubIO.withAttributes[String]("in", null, PubsubWithAttributesJob.timestampAttribute),
        Seq("a", "b", "c").map((_, m))
      )
      .output(
        PubsubIO.withAttributes[String]("out", null, PubsubWithAttributesJob.timestampAttribute)
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

  // unlike attributes, the options don't affect semantic behavior and thus don't need to be passed
  def testPubsubWithOptionsJob(timestampAttribute: Map[String, String], xs: String*): Unit = {
    val m = Map("a" -> "1", "b" -> "2", "c" -> "3") ++ timestampAttribute
    JobTest[PubsubWithOptionsJob.type]
      .args("--input=in", "--output=out")
      .input(
        PubsubIO.withAttributes[String](
          "in",
          null,
          PubsubWithOptionsJob.timestampAttribute,
          PubsubWithOptionsJob.clientOptions
        ),
        Seq("a", "b", "c").map((_, m))
      )
      .output(
        PubsubIO.withAttributes[String](
          "out",
          null,
          PubsubWithOptionsJob.timestampAttribute,
          PubsubWithOptionsJob.clientOptions
        )
      )(coll => coll should containInAnyOrder(xs.map((_, m))))
      .run()
  }

  it should "pass correct PubsubIO with attributes and options" in {
    testPubsubWithOptionsJob(
      Map(PubsubWithOptionsJob.timestampAttribute -> new Instant().toString),
      "aX",
      "bX",
      "cX"
    )
  }

  it should "fail incorrect PubsubIO with attributes and options" in {
    an[AssertionError] should be thrownBy {
      testPubsubWithOptionsJob(
        Map(PubsubWithOptionsJob.timestampAttribute -> new Instant().toString),
        "aX",
        "bX"
      )
    }
    an[AssertionError] should be thrownBy {
      testPubsubWithOptionsJob(
        Map(PubsubWithOptionsJob.timestampAttribute -> new Instant().toString),
        "aX",
        "bX",
        "cX",
        "dX"
      )
    }
  }

  it should "fail PubsubIO with invalid or missing timestamp attribute with options" in {
    an[PipelineExecutionException] should be thrownBy {
      testPubsubWithOptionsJob(
        Map(PubsubWithOptionsJob.timestampAttribute -> "invalidTimestamp"),
        "aX",
        "bX",
        "cX"
      )
    }

    an[PipelineExecutionException] should be thrownBy {
      testPubsubWithOptionsJob(timestampAttribute = Map(), xs = "aX", "bX", "cX")
    }
  }

}

object PubsubJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.read(PubsubIO.string(args("input")))(PubsubIO.ReadParam(PubsubIO.Topic))
      .map(_ + "X")
      .write(PubsubIO.string(args("output")))(PubsubIO.WriteParam())
    sc.run()
    ()
  }
}

object PubsubWithAttributesJob {
  val timestampAttribute = "tsAttribute"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.read(
      PubsubIO.withAttributes[String](args("input"), timestampAttribute = timestampAttribute)
    )(PubsubIO.ReadParam(PubsubIO.Topic))
      .map(kv => (kv._1 + "X", kv._2))
      .write(
        PubsubIO.withAttributes[String](args("output"), timestampAttribute = timestampAttribute)
      )(PubsubIO.WriteParam())
    sc.run()
    ()
  }
}

object PubsubWithOptionsJob {
  val timestampAttribute = "tsAttribute"
  val clientOptions: PubsubOptions = PipelineOptionsFactory.create().as(classOf[PubsubOptions])
  val clientFn: (String, String, PubsubOptions) => PubsubClient = (time, id, opt) => PubsubJsonClient.FACTORY.newClient(time, id, opt)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    clientOptions.setPubsubRootUrl(clientOptions.getPubsubRootUrl) // custom Pubsub endpoint URL
    sc.read(
      PubsubIO.withAttributes[String](
        args("input"),
        timestampAttribute = timestampAttribute,
        clientOptions = clientOptions
      )
    )(PubsubIO.ReadParam(PubsubIO.Topic))
      .map(kv => (kv._1 + "X", kv._2))
      .write(
        PubsubIO.withAttributes[String](
          args("output"),
          timestampAttribute = timestampAttribute,
          clientOptions = clientOptions
        )
      )(PubsubIO.WriteParam(clientFn = clientFn))
    sc.run()
    ()
  }
}
