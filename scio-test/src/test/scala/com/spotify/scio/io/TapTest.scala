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

import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.proto.SimpleV2.{SimplePB => SimplePBV2}
import com.spotify.scio.proto.SimpleV3.{SimplePB => SimplePBV3}
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.{FileUtils, IOUtils}

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

trait TapSpec extends PipelineSpec {
  def verifyTap[T: Coder](tap: Tap[T], expected: Set[T]): Unit = {
    SerializableUtils.ensureSerializable(tap)
    tap.value.toSet shouldBe expected
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder(expected)
    sc.run().waitUntilFinish() // block non-test runner
    ()
  }

  def runWithInMemoryFuture[T](fn: ScioContext => ClosedTap[T]): Tap[T] =
    runWithFuture(ScioContext.forTest())(fn)

  def runWithFileFuture[T](fn: ScioContext => ClosedTap[T]): Tap[T] =
    runWithFuture(ScioContext())(fn)

  def runWithFuture[T](sc: ScioContext)(fn: ScioContext => ClosedTap[T]): Tap[T] = {
    val f = fn(sc)
    val scioResult = sc.run().waitUntilFinish() // block non-test runner
    scioResult.tap(f)
  }

  def withTempDir(test: File => Any): Unit = {
    val dir = Files.createTempDirectory("scio-test-").toFile
    try {
      test(dir)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }
}

class TapTest extends TapSpec {
  val schema: Schema = newGenericRecord(1).getSchema
  implicit def coder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)

  private def makeRecords(sc: ScioContext) =
    sc.parallelize(Seq(1, 2, 3))
      .map(i => (newSpecificRecord(i), newGenericRecord(i)))

  val expectedRecords: Set[(TestRecord, GenericRecord)] =
    Set(1, 2, 3).map(i => (newSpecificRecord(i), newGenericRecord(i)))

  "Future" should "support saveAsInMemoryTap" in {
    val t = runWithInMemoryFuture(makeRecords(_).saveAsInMemoryTap)
    verifyTap(t, expectedRecords)
  }

  it should "support materialize" in {
    val t = runWithFileFuture(makeRecords(_).materialize)
    verifyTap(t, expectedRecords)
  }

  it should "support saveAsAvroFile with SpecificRecord" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(dir.getAbsolutePath)
    }
    verifyTap(t, Set(1, 2, 3).map(newSpecificRecord))
  }

  it should "support saveAsAvroFile with GenericRecord" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsAvroFile(dir.getAbsolutePath, schema = schema)
    }
    verifyTap(t, Set(1, 2, 3).map(newGenericRecord))
  }

  it should "support saveAsAvroFile with reflect record" in withTempDir { dir =>
    import com.spotify.scio.coders.AvroBytesUtil
    implicit val coder = Coder.avroGenericRecordCoder(AvroBytesUtil.schema)

    val tap = runWithFileFuture {
      _.parallelize(Seq("a", "b", "c"))
        .map { s =>
          val record: GenericRecord = new GenericData.Record(AvroBytesUtil.schema)
          record.put("bytes", ByteBuffer.wrap(s.getBytes))
          record
        }
        .saveAsAvroFile(dir.getAbsolutePath, schema = AvroBytesUtil.schema)
    }

    val result = tap
      .map { gr =>
        val bb = gr.get("bytes").asInstanceOf[ByteBuffer]
        new String(bb.array(), bb.position(), bb.limit())
      }

    verifyTap(result, Set("a", "b", "c"))
  }

  it should "support saveAsTextFile" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq("a", "b", "c"))
        .saveAsTextFile(dir.getAbsolutePath)
    }
    verifyTap(t, Set("a", "b", "c"))
  }

  it should "support reading compressed text files" in withTempDir { dir =>
    val nFiles = 10
    val nLines = 100
    val data = Array.fill(nFiles)(Array.fill(nLines)(UUID.randomUUID().toString))

    Seq(
      CompressorStreamFactory.GZIP -> ".gz",
      CompressorStreamFactory.BZIP2 -> ".bz2"
    ).map { case (cType, ext) =>
      val compressDir = new File(dir, cType)
      compressDir.mkdir()
      val suffix = ".txt" + ext
      (0 until nFiles).foreach { f =>
        val file = new File(compressDir, f"part-$f%05d-of-$nFiles%05d$suffix%s")
        val os = new CompressorStreamFactory()
          .createCompressorOutputStream(cType, new FileOutputStream(file))
        IOUtils.write(data(f).mkString("", "\n", "\n"), os, StandardCharsets.UTF_8)
        os.close()
      }

      val params = TextIO.ReadParam(compression = Compression.detect(suffix), suffix = suffix)
      verifyTap(TextTap(compressDir.getAbsolutePath, params), data.flatten.toSet)
    }
  }

  it should "support saveAsProtobuf proto version 2" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    // use java protos otherwise we would have to pull in pb-scala
    def mkProto(t: (String, Long)): SimplePBV2 =
      SimplePBV2
        .newBuilder()
        .setPlays(t._2)
        .setTrackId(t._1)
        .build()
    val t = runWithFileFuture {
      _.parallelize(data)
        .map(mkProto)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = data.map(mkProto).toSet
    verifyTap(t, expected)
  }

  // use java protos otherwise we would have to pull in pb-scala
  private def mkProto3(t: (String, Long)): SimplePBV3 =
    SimplePBV3
      .newBuilder()
      .setPlays(t._2)
      .setTrackId(t._1)
      .build()

  it should "support saveAsProtobuf proto version 3" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val t = runWithFileFuture {
      _.parallelize(data)
        .map(mkProto3)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = data.map(mkProto3).toSet
    verifyTap(t, expected)
  }

  it should "support saveAsProtobuf write with nullableCoders" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val actual = data.map(mkProto3)
    val t = runWithFileFuture { sc =>
      sc.optionsAs[ScioOptions].setNullableCoders(true)
      sc.parallelize(actual)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = actual.toSet
    verifyTap(t, expected)

    val sc = ScioContext()
    sc.protobufFile[SimplePBV3](
      path = dir.getAbsolutePath,
      suffix = ".protobuf.avro"
    ) should containInAnyOrder(expected)
    sc.run()
  }

  it should "support saveAsProtobuf read with nullableCoders" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val actual = data.map(mkProto3)
    val t = runWithFileFuture {
      _.parallelize(actual)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = actual.toSet
    verifyTap(t, expected)

    val sc = ScioContext()
    sc.optionsAs[ScioOptions].setNullableCoders(true)
    sc.protobufFile[SimplePBV3](
      path = dir.getAbsolutePath,
      suffix = ".protobuf.avro"
    ) should containInAnyOrder(expected)
    sc.run()
  }

  it should "keep parent after Tap.map" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .saveAsTextFile(dir.getAbsolutePath)
    }.map(_.toInt)
    verifyTap(t, Set(1, 2, 3))
    t.isInstanceOf[Tap[Int]] shouldBe true
    t.parent.get.isInstanceOf[Tap[_]] shouldBe true
  }

  it should "support waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    val scioResult = sc.run().waitUntilDone()
    scioResult.tap(f).value.toSet shouldBe (1 to 10).toSet
  }

  it should "support nested waitForResult" in {
    val sc = ScioContext()
    val f = sc.parallelize(1 to 10).materialize
    val scioResult = sc.run().waitUntilDone()
    scioResult.tap(f).value.toSet shouldBe (1 to 10).toSet
  }
}
