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

package com.spotify.scio.io

import java.nio.ByteBuffer
import java.nio.file.Files

import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper
import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroUtils.schema
import com.spotify.scio.avro._
import com.spotify.scio.bigquery._
import com.spotify.scio.coders.Coder
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.values.PValue
import org.apache.commons.io.FileUtils

import scala.collection.mutable
import scala.collection.JavaConverters._

object ScioIOTest {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])

  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])
}

class ScioIOTest extends ScioIOSpec {
  import ScioIOTest._

  "AvroIO" should "work with SpecificRecord" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(_.saveAsAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO[TestRecord](_))(_.avroFile(_))(_.saveAsAvroFile(_))
  }

  it should "work with GenericRecord" in {
    import AvroUtils.schema
    implicit val coder = Coder.avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    testTap(xs)(_.saveAsAvroFile(_, schema = schema))(".avro")
    testJobTest(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))
  }

  it should "work with typed Avro" in {
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    val io = (s: String) => AvroIO[AvroRecord](s)
    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))
  }

  it should "work with GenericRecord and a parseFn" in {
    import GenericParseFnAvroFileJob.PartialFieldsAvro
    val xs = (1 to 100).map(PartialFieldsAvro)
    // No test for saveAsAvroFile because parseFn is only for i/p
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[PartialFieldsAvro](_)(
        (gr: GenericRecord) => PartialFieldsAvro(gr.get("int_field").asInstanceOf[Int])
      )
    )(_.saveAsAvroFile(_, schema = schema))
  }

  "ObjectFileIO" should "work" in {
    import ScioIOTest._
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    testTap(xs)(_.saveAsObjectFile(_))(".obj.avro")
    testJobTest[AvroRecord](xs)(ObjectFileIO[AvroRecord](_))(_.objectFile[AvroRecord](_))(
      _.saveAsObjectFile(_)
    )
  }

  "ProtobufIO" should "work" in {
    val xs =
      (1 to 100).map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
    val suffix = ".protobuf.avro"
    testTap(xs)(_.saveAsProtobufFile(_))(suffix)
    testJobTest(xs)(ProtobufIO(_))(_.protobufFile[TrackPB](_))(_.saveAsProtobufFile(_))
  }

  "BigQueryIO" should "work with TableRow" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testJobTest(xs, in = "project:dataset.in_table", out = "project:dataset.out_table")(
      BigQueryIO(_)
    )((sc, s) => sc.bigQueryTable(Table.Spec(s)))(
      (coll, s) => coll.saveAsBigQueryTable(Table.Spec(s))
    )
  }

  it should "work with typed BigQuery" in {
    val xs = (1 to 100).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
    testJobTest(xs, in = "project:dataset.in_table", out = "project:dataset.out_table")(
      BigQueryIO(_)
    )((sc, s) => sc.typedBigQueryTable[BQRecord](Table.Spec(s)))(
      (coll, s) => coll.saveAsTypedBigQueryTable(Table.Spec(s))
    )
  }

  /**
   * The `BigQueryIO`'s write, runs Beam's BQ IO which creates a `Read` Transform to return the
   * insert errors.
   *
   * The `saveAsBigQuery` or `saveAsTypedBigQuery` in Scio is designed to return a `ClosedTap`
   * and by default drops insert errors.
   *
   * The following tests make sure that the dropped insert errors do not appear as an unconsumed
   * read outside the transform writing to Big Query.
   */
  it should "not have unconsumed errors with saveAsBigQuery" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))

    val context = ScioContext()
    context
      .parallelize(xs)
      .saveAsBigQueryTable(Table.Spec("project:dataset.dummy"), createDisposition = CREATE_NEVER)
    // We want to validate on the job graph, and we need not actually execute the pipeline.

    verifyAllReadsConsumed(context)
  }

  it should "not have unconsumed errors with saveAsTypedBigQuery" in {
    val xs = (1 to 100).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))

    val context = ScioContext()
    context
      .parallelize(xs)
      .saveAsTypedBigQueryTable(
        Table.Spec("project:dataset.dummy"),
        createDisposition = CREATE_NEVER
      )
    // We want to validate on the job graph, and we need not actually execute the pipeline.

    verifyAllReadsConsumed(context)
  }

  /**
   * Verify that there are no `Read` Transforms that do not have another transform using it as an
   * input.
   *
   * To do this, we visit all PTransforms, and find the inputs at each stage, and mark those inputs
   * as consumed by putting them in `consumedOutputs`. We also check if each transform is a `Read`
   * and if so we extract them as well.
   *
   * This is copied from Beam's test for UnconsumedReads.
   */
  private def verifyAllReadsConsumed(context: ScioContext): Unit = {
    val consumedOutputs = mutable.HashSet[PValue]()
    val allReads = mutable.HashSet[PValue]()

    context.pipeline.traverseTopologically(
      new PipelineVisitor.Defaults {
        override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
          consumedOutputs ++= node.getInputs.values().asScala

        override def visitValue(
          value: PValue,
          producer: TransformHierarchy#Node
        ): Unit = {
          producer.getTransform match {
            case _: Read.Bounded[_] | _: Read.Unbounded[_] =>
              allReads += value
            case _ =>
          }
        }
      }
    )

    val unconsumedReads = allReads -- consumedOutputs

    unconsumedReads shouldBe empty
    ()
  }

  "TableRowJsonIO" should "work" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testTap(xs)(_.saveAsTableRowJsonFile(_))(".json")
    testJobTest(xs)(TableRowJsonIO(_))(_.tableRowJsonFile(_))(_.saveAsTableRowJsonFile(_))
  }

  "TextIO" should "work" in {
    val xs = (1 to 100).map(_.toString)
    testTap(xs)(_.saveAsTextFile(_))(".txt")
    testJobTest(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))
  }

  "BinaryIO" should "work" in {
    val xs = (1 to 100).map(i => ByteBuffer.allocate(4).putInt(i).array)
    testJobTestOutput(xs)(BinaryIO(_))(_.saveAsBinaryFile(_))
  }

  "BinaryIO" should "output files to $prefix/part-*" in {
    val tmpDir = Files.createTempDirectory("binary-io-")

    val sc = ScioContext()
    sc.parallelize(Seq(ByteBuffer.allocate(4).putInt(1).array)).saveAsBinaryFile(tmpDir.toString)
    sc.run()

    Files
      .list(tmpDir)
      .iterator()
      .asScala
      .filterNot(_.toFile.getName.startsWith("."))
      .map(_.toFile.getName)
      .toSet shouldBe Set("part-00000-of-00001.bin")

    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  "DatastoreIO" should "work" in {
    val xs = (1 to 100).map { x =>
      Entity
        .newBuilder()
        .putProperties("int", DatastoreHelper.makeValue(x).build())
        .build()
    }
    testJobTest(xs)(DatastoreIO(_))(_.datastore(_, null))(_.saveAsDatastore(_))
  }

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
}
