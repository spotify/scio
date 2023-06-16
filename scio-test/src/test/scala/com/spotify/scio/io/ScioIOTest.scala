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
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection.IsBounded
import org.apache.commons.io.FileUtils
import org.joda.time.{Duration, Instant}

import java.io.File
import java.util.UUID
import scala.jdk.CollectionConverters._

object ScioIOTest {
  val TestNumShards = 10
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])
}

trait FileNamePolicySpec[T] extends ScioIOSpec {

  def mkIn(
    sc: ScioContext,
    inFn: ScioContext => SCollection[Int],
    windowInput: Boolean
  ): SCollection[Int] = {
    val input = inFn(sc)
    if (!windowInput) input
    else {
      input
        .timestampBy(x => new Instant(x * 60000L), Duration.ZERO)
        .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
    }
  }

  def testWindowingFilenames(
    inFn: ScioContext => SCollection[Int],
    windowInput: Boolean,
    // (windowed input, tmpDir, isBounded)
    write: (SCollection[Int], String, Boolean) => ClosedTap[T]
  )(
    fileFn: Seq[String] => Unit = _ => ()
  ): Unit = {
    val tmpDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())

    val sc = ScioContext()
    val in: SCollection[Int] = mkIn(sc, inFn, windowInput)
    write(in, tmpDir.getAbsolutePath, in.internal.isBounded == IsBounded.BOUNDED)
    sc.run().waitUntilDone()

    fileFn(listFiles(tmpDir).map(_.getName))
    FileUtils.deleteDirectory(tmpDir)
  }

  def testFails(
    inFn: ScioContext => SCollection[Int],
    failWrites: Seq[SCollection[Int] => ClosedTap[T]] = Seq.empty
  ): Unit = {
    failWrites.foreach { failWrite =>
      a[IllegalArgumentException] should be thrownBy {
        val sc2 = ScioContext()
        failWrite(mkIn(sc2, inFn, true))
        sc2.run().waitUntilDone()
      }
    }
  }

  def suffix: String
  def failSaves: Seq[SCollection[Int] => ClosedTap[T]] = Seq.empty
  def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[T]

  it should "throw when incompatible save parameters are used" in {
    testFails(_.parallelize(1 to 100), failSaves)
  }

  it should "work with an unwindowed collection" in {
    testWindowingFilenames(_.parallelize(1 to 100), false, save()) { files =>
      assert(files.length >= 1)
      all(files) should (startWith("part-") and
        include("-of-") and
        endWith(suffix))
    }
  }

  it should "work with an unwindowed collection with custom prefix and shardNameTemplate" in {
    testWindowingFilenames(
      _.parallelize(1 to 100),
      false,
      save(prefix = testPrefix, shardNameTemplate = testShardNameTemplate)
    ) { files =>
      assert(files.length >= 1)
      all(files) should (startWith("foo-shard-") and
        include("-of-num-shards-") and
        endWith(suffix))
    }
  }

  it should "work with an unwindowed collection with a custom filename policy" in {
    testWindowingFilenames(_.parallelize(1 to 100), false, save(testFilenamePolicySupplier)) {
      files =>
        assert(files.length >= 1)
        all(files) should (startWith("foo-shard-") and
          include("-of-num-shards-") and
          endWith(suffix))
    }
  }

  it should "work with a windowed collection" in {
    testWindowingFilenames(_.parallelize(1 to 100), true, save()) { files =>
      assert(files.length >= 1)
      all(files) should (startWith("part") and
        include("-of-") and
        include("-pane-") and
        endWith(suffix))
    }
  }

  it should "work with a windowed unbounded collection" in {
    val xxx = testStreamOf[Int]
      .addElements(1, (2 to 10): _*)
      .advanceWatermarkToInfinity()
    testWindowingFilenames(_.testStream(xxx), true, save()) { files =>
      assert(files.length == ScioIOTest.TestNumShards)
      all(files) should (startWith("part") and
        include("-of-") and
        include("-pane-") and
        endWith(suffix))
    }
  }

  it should "work with a windowed unbounded collection with a custom filename policy" in {
    val xxx = testStreamOf[Int]
      .addElements(1, (2 to 10): _*)
      .advanceWatermarkToInfinity()
    testWindowingFilenames(_.testStream(xxx), true, save(testFilenamePolicySupplier)) { files =>
      assert(files.length == ScioIOTest.TestNumShards)
      all(files) should (startWith("foo-shard-") and
        include("-of-num-shards-") and
        include("-window") and
        endWith(suffix))
    }
  }
}

class AvroIOFileNamePolicyTest extends FileNamePolicySpec[TestRecord] {
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
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
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

class TextIOFileNamePolicyTest extends FileNamePolicySpec[String] {
  override def suffix: String = ".txt"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[String] = {
    in.map(_.toString)
      .saveAsTextFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[String]] = Seq(
    _.map(_.toString).saveAsTextFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ObjectIOFileNamePolicyTest extends FileNamePolicySpec[ScioIOTest.AvroRecord] {
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
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
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

class BinaryIOFileNamePolicyTest extends FileNamePolicySpec[Nothing] {
  override def suffix: String = ".bin"
  def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[Nothing] = {
    in.map(x => ByteBuffer.allocate(4).putInt(x).array)
      .saveAsBinaryFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[Nothing]] = Seq(
    _.map(x => ByteBuffer.allocate(4).putInt(x).array).saveAsBinaryFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    ),
    _.map(x => ByteBuffer.allocate(4).putInt(x).array).saveAsBinaryFile(
      "nonsense",
      prefix = "blah",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ScioIOTest extends ScioIOSpec {
  import ScioIOTest._
  val TestNumShards = 10
  def relativeFiles(dir: File): Seq[String] =
    listFiles(dir).map(_.toPath.relativize(dir.toPath)).sorted.map(_.toString)

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
    // No test for saveAsAvroFile because parseFn is only for input
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[GenericRecord](_)(identity)
    )(_.saveAsAvroFile(_, schema = schema))
  }

  // covers AvroIO specific, generic, and typed records, ObjectFileIO, and ProtobufIO
  it should "write to the same filenames as previous scio versions when not using a filename policy" in {
    import org.apache.beam.sdk.extensions.avro.io.{AvroIO => BAvroIO}

    val write1 = BAvroIO.write(ScioUtil.classOf[TestRecord])
    val suffix = ".avro"
    val numShards = 10
    val codec = CodecFactory.deflateCodec(6)
    val metadata = Map.empty[String, AnyRef]

    /*
     * pre-scio 0.12.0
     */
    val out1 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val out1TempDir =
      new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    var previousTransform: BAvroIO.Write[TestRecord] = write1
      .to(ScioUtil.pathWithPrefix(out1.getAbsolutePath, null))
      .withSuffix(suffix)
      .withNumShards(numShards)
      .withCodec(codec)
      .withMetadata(metadata.asJava)
    previousTransform = Option(out1TempDir.getAbsolutePath)
      .map(ScioUtil.toResourceId)
      .fold(previousTransform)(previousTransform.withTempDirectory)

    /*
     * current scio
     */
    val out2 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val out2TempDir =
      new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val sr2 = SpecificRecordIO[TestRecord](out2.getAbsolutePath)
    val write2 = BAvroIO.write(ScioUtil.classOf[TestRecord])

    val currentTransform: BAvroIO.Write[TestRecord] = sr2.avroOut(
      write2,
      out2.getAbsolutePath,
      numShards,
      suffix,
      codec,
      metadata,
      null,
      null,
      null,
      false,
      ScioUtil.toResourceId(out2TempDir.getAbsolutePath)
    )

    // verify
    val sc = ScioContext()
    val data = sc.parallelize((1 to 100).map(x => AvroUtils.newSpecificRecord(x)))
    data.applyInternal(previousTransform)
    data.applyInternal(currentTransform)
    sc.run().waitUntilDone()

    relativeFiles(out1) should contain theSameElementsAs relativeFiles(out2)

    FileUtils.deleteDirectory(out1TempDir)
    FileUtils.deleteDirectory(out2TempDir)
  }

  it should "write to the same filenames as previous scio versions when not using a filename policy during a typed write" in {
    val suffix = ".avro"
    val numShards = 10
    val codec = CodecFactory.deflateCodec(6)
    val metadata = Map.empty[String, AnyRef]

    /*
     * pre-scio 0.12.0
     */
    val out1 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val out1TempDir =
      new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val write1 = AvroTyped.writeTransform[AvroRecord]()

    var previousTransform = write1
      .to(ScioUtil.pathWithPrefix(out1.getAbsolutePath, null))
      .withSuffix(suffix)
      .withNumShards(numShards)
      .withCodec(codec)
      .withMetadata(metadata.asJava)
    previousTransform = Option(out1TempDir.getAbsolutePath)
      .map(ScioUtil.toResourceId)
      .fold(previousTransform)(previousTransform.withTempDirectory)

    /*
     * current scio
     */
    val out2 = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val out2TempDir =
      new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())
    val sr2 = AvroTyped.AvroIO[AvroRecord](out2.getAbsolutePath)
    val write2 = AvroTyped.writeTransform[AvroRecord]()

    val currentTransform = sr2.typedAvroOut(
      write2,
      out2.getAbsolutePath,
      numShards,
      suffix,
      codec,
      metadata,
      null,
      null,
      null,
      false,
      ScioUtil.toResourceId(out2TempDir.getAbsolutePath)
    )

    // verify
    val sc = ScioContext()
    val data = sc.parallelize(
      (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    )

    data.applyInternal(previousTransform)
    data.applyInternal(currentTransform)
    sc.run().waitUntilDone()

    relativeFiles(out1) should contain theSameElementsAs relativeFiles(out2)

    FileUtils.deleteDirectory(out1TempDir)
    FileUtils.deleteDirectory(out2TempDir)
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
