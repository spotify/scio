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

import com.spotify.scio.testing._
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.spotify.scio.{CoreSysProps, ScioContext}
import org.apache.beam.sdk.coders.ByteArrayCoder
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values.PCollection.IsBounded
import org.apache.commons.io.FileUtils
import org.joda.time.{Duration, Instant}
import org.typelevel.scalaccompat.annotation.nowarn

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

object ScioIOTest {
  val TestNumShards = 10
}

trait FileNamePolicySpec[T] extends ScioIOSpec {
  import ScioIOTest._

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
      assert(files.length == TestNumShards)
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
      assert(files.length == TestNumShards)
      all(files) should (startWith("foo-shard-") and
        include("-of-num-shards-") and
        include("-window") and
        endWith(suffix))
    }
  }
}

class TextIOFileNamePolicyTest extends FileNamePolicySpec[String] {
  import ScioIOTest._

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
        numShards = if (isBounded) 0 else TestNumShards,
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

class BinaryIOFileNamePolicyTest extends FileNamePolicySpec[Nothing] {
  import ScioIOTest._

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
        numShards = if (isBounded) 0 else TestNumShards,
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

  "TextIO" should "work" in {
    val xs = (1 to 100).map(_.toString)
    testTap(xs)(_.saveAsTextFile(_))(".txt")
    testJobTest(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))
  }

  "BinaryIO" should "work" in {
    val xs = List((1 to 100).toArray.map(_.toByte))
    testJobTest(xs)(BinaryIO(_))(_.binaryFile(_, MaterializeTap.MaterializeReader))(
      _.saveAsBinaryFile(_)
    )
  }

  it should "round-trip records" in {
    import org.apache.beam.sdk.coders.{Coder => BCoder}
    val bac = ByteArrayCoder.of()
    val in = (1 to 10)
      .map(i => 0 to i)
      .map(_.toArray.map(_.toByte))

    val tmpDir = Files.createTempDirectory("binary-io-")
    val sc = ScioContext()
    val xs = in.map(
      CoderUtils.encodeToByteArray(
        bac,
        _,
        BCoder.Context.NESTED: @nowarn("cat=deprecation")
      )
    )
    sc.parallelize(xs).saveAsBinaryFile(tmpDir.toString)
    sc.run()

    import com.google.protobuf.ByteString
    val sc2 = ScioContext()
    val x = sc2
      .binaryFile(tmpDir.toString, MaterializeTap.MaterializeReader)
      .map(ByteString.copyFrom)
    val expected = sc2.parallelize(in).map(ByteString.copyFrom)
    x.intersection(expected).count.tap { cnt =>
      if (cnt != in.size) throw new IllegalStateException(s"Expected ${in.size}, got $cnt")
    }
    x.subtract(expected).count.tap { cnt =>
      if (cnt != 0) throw new IllegalStateException(s"Expected 0, got $cnt")
    }
    sc2.run()

    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "output files to $prefix/part-*" in {
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
