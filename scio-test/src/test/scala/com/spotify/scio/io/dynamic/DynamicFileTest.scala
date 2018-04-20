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

package com.spotify.scio.io.dynamic

import java.nio.file.{Files, Path}

import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.avro._
import com.spotify.scio.testing._
import com.spotify.scio.values.WindowOptions
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._

class DynamicFileTest extends PipelineSpec {

  private def verifyOutput(path: Path, expected: String*): Unit =
    Files.list(path).iterator().asScala.toSet shouldBe expected.map(path.resolve).toSet

  "Dynamic File" should "support text files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1.parallelize(1 to 10)
      .saveAsTextFile(FileDestinations(tmpDir.toString))(s => (s.toInt % 2).toString)
    sc1.close()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.textFile(s"$tmpDir/0/*.txt")
    val lines1 = sc2.textFile(s"$tmpDir/1/*.txt")
    lines0 should containInAnyOrder ((1 to 10).filter(_ % 2 == 0).map(_.toString))
    lines1 should containInAnyOrder ((1 to 10).filter(_ % 2 == 1).map(_.toString))
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support text files with default" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    val fileDestination = FileDestinations(tmpDir.toString, default = "empty")
    sc1.parallelize(Seq.empty[Int])
      .saveAsTextFile(fileDestination)(s => (s.toInt % 2).toString)
    sc1.close()
    verifyOutput(tmpDir, "empty")

    val sc2 = ScioContext()
    sc2.textFile(s"$tmpDir/empty/*.txt") should beEmpty
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support text files with windowing" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    val fileDestination = FileDestinations.windowed(tmpDir.toString, 1)
    sc1.parallelize(1 to 10)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      .timestampBy(x => new Instant(x * 60000), Duration.ZERO)
      .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
      .saveAsTextFile(fileDestination)(s => (s.toInt % 2).toString)
    sc1.close()
    verifyOutput(tmpDir, "0", "1")
    Files.list(tmpDir.resolve("0")).iterator().asScala.size shouldBe 5
    Files.list(tmpDir.resolve("1")).iterator().asScala.size shouldBe 5

    val sc2 = ScioContext()
    val lines0 = sc2.textFile(s"$tmpDir/0/*.txt")
    val lines1 = sc2.textFile(s"$tmpDir/1/*.txt")
    lines0 should containInAnyOrder ((1 to 10).filter(_ % 2 == 0).map(_.toString))
    lines1 should containInAnyOrder ((1 to 10).filter(_ % 2 == 1).map(_.toString))
    (1 to 10).foreach { x =>
      val p = x % 2
      val t1 = new Instant(x * 60000)
      val t2 = t1.plus(60000)
      val lines = sc2.textFile(s"$tmpDir/$p/part-$t1-$t2-*.txt")
      lines should containSingleValue (x.toString)
    }
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support generic Avro files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1.parallelize(1 to 10).map(newGenericRecord)
      .saveAsAvroFile(FileDestinations(tmpDir.toString), schema) { r =>
        (r.get("int_field").toString.toInt % 2).toString
      }
    sc1.close()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.avroFile[GenericRecord](s"$tmpDir/0/*.avro", schema)
    val lines1 = sc2.avroFile[GenericRecord](s"$tmpDir/1/*.avro", schema)
    lines0 should containInAnyOrder ((1 to 10).filter(_ % 2 == 0).map(newGenericRecord))
    lines1 should containInAnyOrder ((1 to 10).filter(_ % 2 == 1).map(newGenericRecord))
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support specific Avro files" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1.parallelize(1 to 10).map(newSpecificRecord)
      .saveAsAvroFile(FileDestinations(tmpDir.toString), schema) { r =>
        (r.getIntField % 2).toString
      }
    sc1.close()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val lines0 = sc2.avroFile[TestRecord](s"$tmpDir/0/*.avro")
    val lines1 = sc2.avroFile[TestRecord](s"$tmpDir/1/*.avro")
    lines0 should containInAnyOrder ((1 to 10).filter(_ % 2 == 0).map(newSpecificRecord))
    lines1 should containInAnyOrder ((1 to 10).filter(_ % 2 == 1).map(newSpecificRecord))
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support Avro files with default" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val sc1 = ScioContext()
    sc1.parallelize(Seq.empty[Int]).map(newSpecificRecord)
      .saveAsAvroFile(FileDestinations(tmpDir.toString, default = "empty"), schema) { r =>
        (r.getIntField % 2).toString
      }
    sc1.close()
    verifyOutput(tmpDir, "empty")

    val sc2 = ScioContext()
    sc2.avroFile[TestRecord](s"$tmpDir/empty/*.avro") should beEmpty
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  it should "support Avro files with windowing" in {
    val tmpDir = Files.createTempDirectory("dynamic-io-")
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    val fileDestination = FileDestinations.windowed(tmpDir.toString, 1)
    sc1.parallelize(1 to 10).map(newSpecificRecord)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      .timestampBy(x => new Instant(x.getIntField * 60000), Duration.ZERO)
      .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
      .saveAsAvroFile(fileDestination, schema)(r => (r.getIntField % 2).toString)
    sc1.close()
    verifyOutput(tmpDir, "0", "1")
    Files.list(tmpDir.resolve("0")).iterator().asScala.size shouldBe 5
    Files.list(tmpDir.resolve("1")).iterator().asScala.size shouldBe 5

    val sc2 = ScioContext()
    val records0 = sc2.avroFile[TestRecord](s"$tmpDir/0/*.avro")
    val records1 = sc2.avroFile[TestRecord](s"$tmpDir/1/*.avro")
    records0 should containInAnyOrder ((1 to 10).filter(_ % 2 == 0).map(newSpecificRecord))
    records1 should containInAnyOrder ((1 to 10).filter(_ % 2 == 1).map(newSpecificRecord))
    (1 to 10).foreach { x =>
      val p = x % 2
      val t1 = new Instant(x * 60000)
      val t2 = t1.plus(60000)
      val records = sc2.avroFile[TestRecord](s"$tmpDir/$p/part-$t1-$t2-*.avro")
      records should containSingleValue (newSpecificRecord(x))
    }
    sc2.close()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

}
