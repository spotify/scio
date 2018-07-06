/*
 * Copyright 2016 Spotify AB.
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

import java.io._
import java.nio.ByteBuffer
import java.util.UUID

import com.google.api.client.util.Charsets
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.proto.SimpleV2.{SimplePB => SimplePBV2}
import com.spotify.scio.proto.SimpleV3.{SimplePB => SimplePBV3}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.concurrent.Future
import scala.reflect.ClassTag

trait TapSpec extends PipelineSpec {
  def verifyTap[T: ClassTag](tap: Tap[T], expected: Set[T]): Unit = {
    SerializableUtils.ensureSerializable(tap)
    tap.value.toSet shouldBe expected
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder (expected)
    sc.close().waitUntilFinish()  // block non-test runner
  }

  def runWithInMemoryFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] =
    runWithFuture(ScioContext.forTest())(fn)

  def runWithFileFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] =
    runWithFuture(ScioContext())(fn)

  def runWithFuture[T](sc: ScioContext)(fn: ScioContext => Future[Tap[T]]): Tap[T] = {
    val f = fn(sc)
    sc.close().waitUntilFinish()  // block non-test runner
    f.waitForResult()
  }

  def tmpDir: File = new File(
    new File(sys.props("java.io.tmpdir")),
    "scio-test-" + UUID.randomUUID())
}

class TapTest extends TapSpec {

  private def makeRecords(sc: ScioContext) =
    sc.parallelize(Seq(1, 2, 3))
      .map(i => (newSpecificRecord(i), newGenericRecord(i)))

  val expectedRecords = Set(1, 2, 3).map(i => (newSpecificRecord(i), newGenericRecord(i)))

  "Future" should "support saveAsInMemoryTap" in {
    val t = runWithInMemoryFuture { makeRecords(_).saveAsInMemoryTap }
    verifyTap(t, expectedRecords)
  }

  it should "update isCompleted with testId" in {
    val sc = ScioContext.forTest()
    val f = sc.parallelize(Seq(1, 2, 3))
      .map(newSpecificRecord)
      .saveAsInMemoryTap
    f.isCompleted shouldBe false
    sc.close().waitUntilFinish()  // block non-test runner
    f.isCompleted shouldBe true
  }

  it should "update isCompleted without testId" in {
    val dir = tmpDir
    val sc = ScioContext()
    val f = sc.parallelize(Seq(1, 2, 3))
      .map(newSpecificRecord)
      .saveAsAvroFile(dir.getPath)
    f.isCompleted shouldBe false
    sc.close().waitUntilFinish()  // block non-test runner
    f.isCompleted shouldBe true
    FileUtils.deleteDirectory(dir)
  }

  it should "support materialize" in {
    val t = runWithFileFuture { makeRecords(_).materialize }
    verifyTap(t, expectedRecords)
  }

  it should "support saveAsAvroFile with SpecificRecord" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(dir.getPath)
    }
    verifyTap(t, Set(1, 2, 3).map(newSpecificRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsAvroFile with GenericRecord" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsAvroFile(dir.getPath, schema = newGenericRecord(1).getSchema)
    }
    verifyTap(t, Set(1, 2, 3).map(newGenericRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsAvroFile with reflect record" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c"))
        .map(s => ByteBuffer.wrap(s.getBytes))
        .saveAsAvroFile(dir.getPath, schema = new Schema.Parser().parse("\"bytes\""))
    }.map(bb => new String(bb.array(), bb.position(), bb.limit()))
    verifyTap(t, Set("a", "b", "c"))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsTextFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c"))
        .saveAsTextFile(dir.getPath)
    }
    verifyTap(t, Set("a", "b", "c"))
    FileUtils.deleteDirectory(dir)
  }

  it should "support reading compressed text files" in {
    val nFiles = 10
    val nLines = 100
    val data = Array.fill(nFiles)(Array.fill(nLines)(UUID.randomUUID().toString))
    for ((cType, ext) <- Seq(("gz", "gz"), ("bzip2", "bz2"))) {
      val dir = tmpDir
      dir.mkdir()
      for (i <- 0 until nFiles) {
        val file = new File(dir, "part-%05d-%05d.%s".format(i, nFiles, ext))
        val os = new CompressorStreamFactory()
          .createCompressorOutputStream(cType, new FileOutputStream(file))
        data(i).foreach(l => IOUtils.write(l + "\n", os, Charsets.UTF_8))
        os.close()
      }
      verifyTap(TextTap(ScioUtil.addPartSuffix(dir.getPath, ext)), data.flatten.toSet)
      FileUtils.deleteDirectory(dir)
    }
  }

  it should "support saveAsProtobuf proto version 2" in {
    val dir = tmpDir
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    // use java protos otherwise we would have to pull in pb-scala
    def mkProto(t: (String, Int)): SimplePBV2 = SimplePBV2.newBuilder()
                                                          .setPlays(t._2)
                                                          .setTrackId(t._1)
                                                          .build()
    val t = runWithFileFuture {
      _
        .parallelize(data)
        .map(mkProto)
        .saveAsProtobufFile(dir.getPath)
    }
    val expected = data.map(mkProto).toSet
    verifyTap(t, expected)
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsProtobuf proto version 3" in {
    val dir = tmpDir
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    // use java protos otherwise we would have to pull in pb-scala
    def mkProto(t: (String, Int)): SimplePBV3 = SimplePBV3.newBuilder()
                                                          .setPlays(t._2)
                                                          .setTrackId(t._1)
                                                          .build()
    val t = runWithFileFuture {
      _
        .parallelize(data)
        .map(mkProto)
        .saveAsProtobufFile(dir.getPath)
    }
    val expected = data.map(mkProto).toSet
    verifyTap(t, expected)
    FileUtils.deleteDirectory(dir)
  }

  it should "keep parent after Tap.map" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .saveAsTextFile(dir.getPath)
    }.map(_.toInt)
    verifyTap(t, Set(1, 2, 3))
    t.isInstanceOf[Tap[Int]] shouldBe true
    t.parent.get.isInstanceOf[Tap[_]] shouldBe true
    FileUtils.deleteDirectory(dir)
  }

  it should "support waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    sc.close()
    f.waitForResult().value.toSet shouldBe (1 to 10).toSet
  }

  it should "support nested waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    sc.close()
    import scala.concurrent.ExecutionContext.Implicits.global
    Future(f).waitForResult().value.toSet shouldBe (1 to 10).toSet
  }

}
