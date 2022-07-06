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

package com.spotify.scio.io.dynamic

import java.nio.file.{Files, Path}

import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.proto.SimpleV2.SimplePB
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils
import org.joda.time.{Duration, Instant}

import scala.jdk.CollectionConverters._

class DynamicFileTest extends PipelineSpec {
  private def verifyOutput(path: Path, expected: String*): Unit = {
    val actual = Files
      .list(path)
      .iterator()
      .asScala
      .filterNot(_.toFile.getName.startsWith("."))
      .toSet
    actual shouldBe expected.map(path.resolve).toSet
    ()
  }

  "Dynamic File" should "support text files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1
      .parallelize(1 to 10)
      .saveAsDynamicTextFile(tmpDir.toString)(s => (s.toInt % 2).toString)
    sc1.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.textFile(s"$tmpDir/0/*.txt")
    val lines1 = sc2.textFile(s"$tmpDir/1/*.txt")
    lines0 should containInAnyOrder((1 to 10).filter(_ % 2 == 0).map(_.toString))
    lines1 should containInAnyOrder((1 to 10).filter(_ % 2 == 1).map(_.toString))
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support text files with windowing" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    sc1
      .parallelize(1 to 10)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      .timestampBy(x => new Instant(x * 60000L), Duration.ZERO)
      .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
      .saveAsDynamicTextFile(tmpDir.toString, 1)(s => (s.toInt % 2).toString)
    sc1.run()
    verifyOutput(tmpDir, "0", "1")
    Files.list(tmpDir.resolve("0")).iterator().asScala.size shouldBe 5
    Files.list(tmpDir.resolve("1")).iterator().asScala.size shouldBe 5

    val sc2 = ScioContext()
    val lines0 = sc2.textFile(s"$tmpDir/0/*.txt")
    val lines1 = sc2.textFile(s"$tmpDir/1/*.txt")
    lines0 should containInAnyOrder((1 to 10).filter(_ % 2 == 0).map(_.toString))
    lines1 should containInAnyOrder((1 to 10).filter(_ % 2 == 1).map(_.toString))
    (1 to 10).foreach { x =>
      val p = x % 2
      val t1 = new Instant(x * 60000L)
      val t2 = t1.plus(60000L)
      val lines = sc2.textFile(s"$tmpDir/$p/part-$t1-$t2-*.txt")
      lines should containSingleValue(x.toString)
    }
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support generic Avro files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    implicit val coder = Coder.avroGenericRecordCoder(schema)
    sc1
      .parallelize(1 to 10)
      .map(newGenericRecord)
      .saveAsDynamicAvroFile(tmpDir.toString, schema = schema) { r =>
        (r.get("int_field").toString.toInt % 2).toString
      }
    sc1.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.avroFile(s"$tmpDir/0/*.avro", schema)
    val lines1 = sc2.avroFile(s"$tmpDir/1/*.avro", schema)
    lines0 should containInAnyOrder((1 to 10).filter(_ % 2 == 0).map(newGenericRecord))
    lines1 should containInAnyOrder((1 to 10).filter(_ % 2 == 1).map(newGenericRecord))
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support specific Avro files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1
      .parallelize(1 to 10)
      .map(newSpecificRecord)
      .saveAsDynamicAvroFile(tmpDir.toString)(r => (r.getIntField % 2).toString)
    sc1.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.avroFile[TestRecord](s"$tmpDir/0/*.avro")
    val lines1 = sc2.avroFile[TestRecord](s"$tmpDir/1/*.avro")
    lines0 should containInAnyOrder((1 to 10).filter(_ % 2 == 0).map(newSpecificRecord))
    lines1 should containInAnyOrder((1 to 10).filter(_ % 2 == 1).map(newSpecificRecord))
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support Avro files with windowing" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    sc1
      .parallelize(1 to 10)
      .map(newSpecificRecord)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      .timestampBy(x => new Instant(x.getIntField * 60000L), Duration.ZERO)
      .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
      .saveAsDynamicAvroFile(tmpDir.toString, 1)(r => (r.getIntField % 2).toString)
    sc1.run()
    verifyOutput(tmpDir, "0", "1")
    Files.list(tmpDir.resolve("0")).iterator().asScala.size shouldBe 5
    Files.list(tmpDir.resolve("1")).iterator().asScala.size shouldBe 5

    val sc2 = ScioContext()
    val records0 = sc2.avroFile[TestRecord](s"$tmpDir/0/*.avro")
    val records1 = sc2.avroFile[TestRecord](s"$tmpDir/1/*.avro")
    records0 should containInAnyOrder((1 to 10).filter(_ % 2 == 0).map(newSpecificRecord))
    records1 should containInAnyOrder((1 to 10).filter(_ % 2 == 1).map(newSpecificRecord))
    (1 to 10).foreach { x =>
      val p = x % 2
      val t1 = new Instant(x * 60000L)
      val t2 = t1.plus(60000L)
      val records = sc2.avroFile[TestRecord](s"$tmpDir/$p/part-$t1-$t2-*.avro")
      records should containSingleValue(newSpecificRecord(x))
    }
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support Proto files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()

    val mkProto = (x: Long) => SimplePB.newBuilder().setPlays(x).setTrackId(s"track$x").build()
    sc1
      .parallelize(1L to 10L)
      .map(mkProto)
      .saveAsDynamicProtobufFile(tmpDir.toString)(r => (r.getPlays % 2).toString)
    sc1.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.protobufFile[SimplePB](s"$tmpDir/0/*.protobuf")
    val lines1 = sc2.protobufFile[SimplePB](s"$tmpDir/1/*.protobuf")
    lines0 should containInAnyOrder((1L to 10L).filter(_ % 2 == 0).map(mkProto))
    lines1 should containInAnyOrder((1L to 10L).filter(_ % 2 == 1).map(mkProto))
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }
}
