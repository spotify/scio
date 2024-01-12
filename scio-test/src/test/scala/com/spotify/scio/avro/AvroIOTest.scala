/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, FileNamePolicySpec, ScioIOTest}
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic._
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.joda.time.LocalDate

import java.io.File

object AvroIOTest {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])
}

class AvroIOFileNamePolicyTest extends FileNamePolicySpec[TestRecord] {
  import ScioIOTest._
  override def suffix: String = ".avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[TestRecord] = {
    in.map(AvroUtils.newSpecificRecord)
      .saveAsAvroFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[TestRecord]] = Seq(
    _.map(AvroUtils.newSpecificRecord).saveAsAvroFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ObjectIOFileNamePolicyTest extends FileNamePolicySpec[AvroIOTest.AvroRecord] {
  import AvroIOTest._
  import ScioIOTest._

  override def suffix: String = ".obj.avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[AvroRecord] = {
    in.map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
      .saveAsObjectFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[AvroRecord]] = Seq(
    _.map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList)).saveAsObjectFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ProtobufIOFileNamePolicyTest extends FileNamePolicySpec[TrackPB] {
  import ScioIOTest._
  override def suffix: String = ".protobuf.avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[TrackPB] = {
    in.map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
      .saveAsProtobufFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[TrackPB]] = Seq(
    _.map(x => TrackPB.newBuilder().setTrackId(x.toString).build()).saveAsProtobufFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class AvroIOTest extends ScioIOSpec {
  import AvroIOTest._
  def relativeFiles(dir: File): Seq[String] =
    listFiles(dir).map(_.toPath.relativize(dir.toPath)).sorted.map(_.toString)

  "GenericAvroIO" should "work with GenericRecord" in {
    import AvroUtils.schema
    implicit val coder = avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    testTap(xs)(_.saveAsAvroFile(_, schema = schema))(".avro")
    testJobTest(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))
  }

  "SpecificAvroIO" should "work with SpecificRecord" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(_.saveAsAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO[TestRecord](_))(_.avroFile(_))(_.saveAsAvroFile(_))
  }

  "ParseAvroIO" should "work with GenericRecord and a parseFn" in {
    import AvroUtils.schema
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    // No test for saveAsAvroFile because parseFn is only for input
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[GenericRecord](_)(identity)
    )(_.saveAsAvroFile(_, schema = schema))
  }

  it should "work with GenericRecord with logical-type" in {
    val schema = SchemaBuilder
      .builder()
      .record("LogicalTypeTestRecord")
      .fields()
      .name("date")
      .`type`(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
      .withDefault(0)
      .endRecord()

    val datumFactory = new AvroDatumFactory.GenericDatumFactory {
      override def apply(writer: Schema): DatumWriter[GenericRecord] = {
        val data = new GenericData()
        data.addLogicalTypeConversion(new TimeConversions.DateConversion)
        new GenericDatumWriter[GenericRecord](writer, data)
      }

      override def apply(writer: Schema, reader: Schema): DatumReader[GenericRecord] = {
        val data = new GenericData()
        data.addLogicalTypeConversion(new TimeConversions.DateConversion)
        new GenericDatumReader[GenericRecord](writer, reader, data)
      }
    }

    implicit val coder: Coder[GenericRecord] = avroCoder(datumFactory, schema)
    val xs = (1 to 100)
      .map(i => new LocalDate(i.toLong)) // use LocalDate instead of int to test logical-type
      .map[GenericRecord](d => new GenericRecordBuilder(schema).set("date", d).build())

    // No test for saveAsAvroFile because parseFn is only for input
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[GenericRecord](_, datumFactory = datumFactory)(identity)
    )(_.saveAsAvroFile(_, schema = schema, datumFactory = datumFactory))
  }

  "TypedAvroIO" should "work with typed Avro" in {
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    val io = (s: String) => AvroIO[AvroRecord](s)
    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))
  }

  "ObjectFileIO" should "work" in {
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

}
