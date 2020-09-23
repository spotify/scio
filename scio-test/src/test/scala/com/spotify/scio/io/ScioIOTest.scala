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

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroUtils.schema
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils

import scala.jdk.CollectionConverters._

object ScioIOTest {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])
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
    implicit val coder = Coder.avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    // No test for saveAsAvroFile because parseFn is only for i/p
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[GenericRecord](_)(identity)
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

}
