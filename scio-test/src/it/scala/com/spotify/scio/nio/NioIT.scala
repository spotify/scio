/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.nio

import java.io.File
import java.util.UUID

import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.bigquery._
import com.spotify.scio.io.Tap
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils

import scala.concurrent.Future
import scala.reflect.ClassTag

object NioIT {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])

  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])
}

class NioIT extends PipelineSpec {

  private def testTap[T: ClassTag](xs: Seq[T])
                                  (ioFn: String => ScioIO[T])
                                  (readFn: (ScioContext, String) => SCollection[T])
                                  (writeFn: (SCollection[T], String) => Future[Tap[T]])
                                  (suffix: String): Unit = {
    val tmpDir = new File(
      new File(sys.props("java.io.tmpdir")),
      "scio-test-" + UUID.randomUUID())

    val sc = ScioContext()
    val data = sc.parallelize(xs)
    val future = writeFn(data, tmpDir.getAbsolutePath)
    sc.close().waitUntilDone()
    val tap = future.waitForResult()

    tap.value.toSeq should contain theSameElementsAs xs
    tap.open(ScioContext()) should containInAnyOrder(xs)
    all(tmpDir.listFiles().map(_.toString)) should endWith (suffix)
    FileUtils.deleteDirectory(tmpDir)
  }

  private def testJobTest[T: ClassTag](xs: Seq[T], in: String = "in", out: String = "out")
                                      (ioFn: String => ScioIO[T])
                                      (readFn: (ScioContext, String) => SCollection[T])
                                      (writeFn: (SCollection[T], String) => Future[Tap[T]])
  : Unit = {
    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      val data = readFn(sc, argz("input"))
      writeFn(data, argz("output"))
      sc.close()
    }

    val builder = com.spotify.scio.testing.JobTest("null")
      .input(ioFn(in), xs)
      .output(ioFn(out))(_ should containInAnyOrder (xs))
    builder.setUp()
    runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
    builder.tearDown()

    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing.JobTest("null")
        .input(CustomIO[T](in), xs)
        .output(ioFn(out))(_ should containInAnyOrder (xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test input: ${ioFn(in)}, " +
      s"available: [com.spotify.scio.nio.CustomIO($in)]"

    the [IllegalArgumentException] thrownBy {
      val builder = com.spotify.scio.testing.JobTest("null")
        .input(ioFn(in), xs)
        .output(CustomIO[T](out))(_ should containInAnyOrder (xs))
      builder.setUp()
      runMain(Array(s"--input=$in", s"--output=$out") :+ s"--appName=${builder.testId}")
      builder.tearDown()
    } should have message s"requirement failed: Missing test output: ${ioFn(out)}, " +
      s"available: [com.spotify.scio.nio.CustomIO($out)]"
    // scalastyle:on no.whitespace.before.left.bracket
  }

  import NioIT._

  "AvroIO" should "work with SpecificRecord" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(AvroIO(_))(_.avroFile(_))(_.saveAsAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO(_))(_.avroFile(_))(_.saveAsAvroFile(_))
  }

  it should "work with GenericRecord" in {
    import AvroUtils.schema
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    testTap(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))(".avro")
    testJobTest(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))
  }

  it should "work with typed Avro" in {
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    val io = avro.nio.Typed.AvroIO[AvroRecord] _
    testTap(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))
  }

  "ObjectFileIO" should "work" in {
    import NioIT._
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    testTap(xs)(ObjectFileIO(_))(_.objectFile(_))(_.saveAsObjectFile(_))(".obj.avro")
    testJobTest(xs)(ObjectFileIO(_))(_.objectFile(_))(_.saveAsObjectFile(_))
  }

  "ProtobufIO" should "work" in {
    val xs = (1 to 100).map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
    val suffix = ".protobuf.avro"
    testTap(xs)(ProtobufIO(_))(_.protobufFile[TrackPB](_))(_.saveAsProtobufFile(_))(suffix)
    testJobTest(xs)(ProtobufIO(_))(_.protobufFile[TrackPB](_))(_.saveAsProtobufFile(_))
  }

  "BigQueryIO" should "work with TableRow" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testJobTest(xs)(BigQueryIO(_))(_.bigQueryTable(_))(_.saveAsBigQuery(_))
  }

  it should "work with typed BigQuery" in {
    val xs = (1 to 100).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
    val in = "project:dataset.input"
    val out = "project:dataset.output"
    testJobTest(xs, in, out)(BigQueryIO(_))(_.typedBigQuery(_))(_.saveAsTypedBigQuery(_))
  }

  "TableRowJsonIO" should "work" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testTap(xs)(TableRowJsonIO(_))(_.tableRowJsonFile(_))(_.saveAsTableRowJsonFile(_))(".json")
    testJobTest(xs)(TableRowJsonIO(_))(_.tableRowJsonFile(_))(_.saveAsTableRowJsonFile(_))
  }

  "TextIO" should "work" in {
    val xs = (1 to 100).map(_.toString)
    testTap(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))(".txt")
    testJobTest(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))
  }

  "DatastoreIO" should "work" in {
    val xs = (1 to 100).map { x =>
      Entity.newBuilder()
        .putProperties("int", DatastoreHelper.makeValue(x).build())
        .build()
    }
      testJobTest(xs)(DatastoreIO(_))(_.datastore(_, null))(_.saveAsDatastore(_))
    }

  "PubSubIO" should "work with subscription" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubSubIO(_))(_.pubsubSubscription(_))(_.saveAsPubsub(_))
  }

  it should "work with topic" in {
    val xs = (1 to 100).map(_.toString)
    testJobTest(xs)(PubSubIO(_))(_.pubsubTopic(_))(_.saveAsPubsub(_))
  }

  it should "work with subscription and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    val io = (s: String) => PubSubIO[(String, Map[String, String])](s)
    testJobTest(xs)(io)(_.pubsubSubscriptionWithAttributes(_))(_.saveAsPubsubWithAttributes(_))
  }

  it should "work with topic and attributes" in {
    val xs = (1 to 100).map(x => (x.toString, Map.empty[String, String]))
    val io = (s: String) => PubSubIO[(String, Map[String, String])](s)
    testJobTest(xs)(io)(_.pubsubTopicWithAttributes(_))(_.saveAsPubsubWithAttributes(_))
  }

}
