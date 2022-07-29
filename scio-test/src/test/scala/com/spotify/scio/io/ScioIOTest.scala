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
import com.spotify.scio.{CoreSysProps, ScioContext}
import com.spotify.scio.avro.AvroUtils.schema
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.ScioUtil.FilenamePolicyCreator
import com.spotify.scio.values.SCollection
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.UUID
import scala.jdk.CollectionConverters._

object ScioIOTest {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])
}

class ScioIOTest extends ScioIOSpec {
  import ScioIOTest._
//
//  "AvroIO" should "work with SpecificRecord" in {
//    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
//    testTap(xs)(_.saveAsAvroFile(_))(".avro")
//    testJobTest(xs)(AvroIO[TestRecord](_))(_.avroFile(_))(_.saveAsAvroFile(_))
//  }
//
//  it should "work with GenericRecord" in {
//    import AvroUtils.schema
//    implicit val coder = Coder.avroGenericRecordCoder(schema)
//    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
//    testTap(xs)(_.saveAsAvroFile(_, schema = schema))(".avro")
//    testJobTest(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))
//  }
//
//  it should "work with typed Avro" in {
//    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
//    val io = (s: String) => AvroIO[AvroRecord](s)
//    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
//    testJobTest(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))
//  }
//
//  it should "work with GenericRecord and a parseFn" in {
//    implicit val coder = Coder.avroGenericRecordCoder(schema)
//    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
//    // No test for saveAsAvroFile because parseFn is only for input
//    testJobTest(xs)(AvroIO(_))(
//      _.parseAvroFile[GenericRecord](_)(identity)
//    )(_.saveAsAvroFile(_, schema = schema))
//  }
//
//  // covers AvroIO specific, generic, and typed records, ObjectFileIO, and ProtobufIO
//  "AvroIO.avroOut" should "without a filename policy have the same behavior as previous scio versions" in {
//    import org.apache.beam.sdk.io.{AvroIO => BAvroIO}
//
//    val write1 = BAvroIO.write(ScioUtil.classOf[TestRecord])
//    val suffix = ".avro"
//    val numShards = 10
//    val codec = CodecFactory.deflateCodec(6)
//    val metadata = Map.empty[String, AnyRef]
//
//    /*
//     * pre-scio 0.12.0
//     */
//    val out1 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
//    val out1TempDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID()).getAbsolutePath
//    var previousTransform: BAvroIO.Write[TestRecord] = write1
//      .to(ScioUtil.pathWithShards(out1.getAbsolutePath))
//      .withSuffix(suffix)
//      .withNumShards(numShards)
//      .withCodec(codec)
//      .withMetadata(metadata.asJava)
//    previousTransform = Option(out1TempDir)
//      .map(ScioUtil.toResourceId)
//      .fold(previousTransform)(previousTransform.withTempDirectory)
//
//    /*
//     * current scio
//     */
//    val out2 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
//    val out2TempDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
//    val sr2 = SpecificRecordIO[TestRecord](out2.getAbsolutePath)
//    val write2 = BAvroIO.write(ScioUtil.classOf[TestRecord])
//
//    val currentTransform: BAvroIO.Write[TestRecord] = sr2.avroOut(
//      write2,
//      out2.getAbsolutePath,
//      numShards,
//      suffix,
//      codec,
//      metadata,
//      null,
//      ScioUtil.toResourceId(out2TempDir.getAbsolutePath),
//      null,
//      false
//    )
//
//    // verify
//    val sc = ScioContext()
//    val data = sc.parallelize((1 to 100).map(x => AvroUtils.newSpecificRecord(x)))
//    data.applyInternal(previousTransform)
//    data.applyInternal(currentTransform)
//    sc.run().waitUntilDone()
//
//    def relativeFiles(dir: File) = listFiles(dir).map(_.stripPrefix(dir.getAbsolutePath)).sorted
//
//    relativeFiles(out1) should contain theSameElementsAs relativeFiles(out2)
//  }
//
//  def saveAvro(
//    filenamePolicyCreator: FilenamePolicyCreator = null
//  )(
//    in: SCollection[Int],
//    tmpDir: String,
//    isBounded: Boolean
//  ): ClosedTap[TestRecord] = {
//    in.map(AvroUtils.newSpecificRecord)
//      .saveAsAvroFile(
//        tmpDir,
//        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
//        numShards = if (isBounded) 0 else 10,
//        filenamePolicyCreator = filenamePolicyCreator
//      )
//  }
//
//  "AvroIO" should "work with an unwindowed collection" in {
//    testWindowingFilenames(_.parallelize(1 to 100), false, saveAvro())(
//      all(_) should (include("/part-") and include("-of-") and include(".avro"))
//    )
//  }
//
//  it should "work with an unwindowed collection with a custom filename policy" in {
//    testWindowingFilenames(_.parallelize(1 to 100), false, saveAvro(testFilenamePolicyCreator))(
//      all(_) should (include("/foo-shard-") and include("-of-numShards-") and include(".avro"))
//    )
//  }
//
//  it should "work with a windowed collection" in {
//    testWindowingFilenames(_.parallelize(1 to 100), true, saveAvro())(
//      all(_) should (include("/part") and include("-of-") and include("-pane-") and include(".avro"))
//    )
//  }
//
//  it should "work with a windowed unbounded collection" in {
//    val xxx = testStreamOf[Int]
//      .addElements(1, (2 to 10): _*)
//      .advanceWatermarkToInfinity()
//    testWindowingFilenames(_.testStream(xxx), true, saveAvro())(
//      all(_) should (include("/part") and include("-of-") and include("-pane-") and include(".avro"))
//    )
//  }
//
//  it should "work with a windowed unbounded collection with a custom filename policy" in {
//    val xxx = testStreamOf[Int]
//      .addElements(1, (2 to 10): _*)
//      .advanceWatermarkToInfinity()
//    testWindowingFilenames(_.testStream(xxx), true, saveAvro(testFilenamePolicyCreator))(
//      all(_) should (include("/foo-shard-") and include("-of-numShards-") and include("-window") and include(".avro"))
//    )
//  }
//
//  "ObjectFileIO" should "work" in {
//    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
//    testTap(xs)(_.saveAsObjectFile(_))(".obj.avro")
//    testJobTest[AvroRecord](xs)(ObjectFileIO[AvroRecord](_))(_.objectFile[AvroRecord](_))(
//      _.saveAsObjectFile(_)
//    )
//  }
//
//  "ProtobufIO" should "work" in {
//    val xs =
//      (1 to 100).map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
//    val suffix = ".protobuf.avro"
//    testTap(xs)(_.saveAsProtobufFile(_))(suffix)
//    testJobTest(xs)(ProtobufIO(_))(_.protobufFile[TrackPB](_))(_.saveAsProtobufFile(_))
//  }
//
  "TextIO" should "work" in {
    val xs = (1 to 100).map(_.toString)
    testTap(xs)(_.saveAsTextFile(_))(".txt")
    testJobTest(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))
  }
//
//  def saveText(
//    filenamePolicyCreator: FilenamePolicyCreator = null
//  )(
//    in: SCollection[Int],
//    tmpDir: String,
//    isBounded: Boolean
//  ): ClosedTap[String] = {
//    in.map(_.toString)
//      .saveAsTextFile(
//        tmpDir,
//        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
//        numShards = if (isBounded) 0 else 10,
//        filenamePolicyCreator = filenamePolicyCreator
//      )
//  }
//
//  it should "work with an unwindowed collection" in {
//    testWindowingFilenames(_.parallelize(1 to 100), false, saveText())(
//      files => {
//        files should haveSize(10)
//        all(files) should (include("/part-") and include("-of-") and include(".txt"))
//      }
//    )
//  }
//
//  it should "work with an unwindowed collection with a custom filename policy" in {
//    testWindowingFilenames(_.parallelize(1 to 100), false, saveText(testFilenamePolicyCreator))(
//      all(_) should (include("/foo-shard-") and include("-of-numShards-") and include(".txt"))
//    )
//  }
//
//  it should "work with a windowed collection" in {
//    testWindowingFilenames(_.parallelize(1 to 100), true, saveText())(
//      all(_) should (include("/part") and include("-of-") and include("-pane-") and include(".txt"))
//    )
//  }
//
//  it should "work with a windowed unbounded collection" in {
//    val xxx = testStreamOf[Int]
//      .addElements(1, (2 to 10): _*)
//      .advanceWatermarkToInfinity()
//    testWindowingFilenames(_.testStream(xxx), true, saveText())(
//      all(_) should (include("/part") and include("-of-") and include("-pane-") and include(".txt"))
//    )
//  }
//
//  it should "work with a windowed unbounded collection with a custom filename policy" in {
//    val xxx = testStreamOf[Int]
//      .addElements(1, (2 to 10): _*)
//      .advanceWatermarkToInfinity()
//    testWindowingFilenames(_.testStream(xxx), true, saveText(testFilenamePolicyCreator))(
//      all(_) should (include("/foo-shard-") and include("-of-numShards-") and include("-window") and include(".txt"))
//    )
//  }
//
//  "BinaryIO" should "work" in {
//    val xs = (1 to 100).map(i => ByteBuffer.allocate(4).putInt(i).array)
//    testJobTestOutput(xs)(BinaryIO(_))(_.saveAsBinaryFile(_))
//  }
//
//  "BinaryIO" should "output files to $prefix/part-*" in {
//    val tmpDir = Files.createTempDirectory("binary-io-")
//
//    val sc = ScioContext()
//    sc.parallelize(Seq(ByteBuffer.allocate(4).putInt(1).array)).saveAsBinaryFile(tmpDir.toString)
//    sc.run()
//
//    Files
//      .list(tmpDir)
//      .iterator()
//      .asScala
//      .filterNot(_.toFile.getName.startsWith("."))
//      .map(_.toFile.getName)
//      .toSet shouldBe Set("part-00000-of-00001.bin")
//
//    FileUtils.deleteDirectory(tmpDir.toFile)
//  }
}
