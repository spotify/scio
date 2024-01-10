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

package com.spotify.scio.extra.csv.dynamic

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.csv._
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import kantan.csv._
import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Random

case class TypedRecord(
  int: Int = 0,
  long: Long = 0L,
  float: Float = 0f,
  bool: Boolean = false,
  string: String = ""
)

object TypedRecord {

  val cscHeaderCodec: HeaderCodec[TypedRecord] = HeaderCodec.caseCodec(
    "int",
    "long",
    "float",
    "bool",
    "string"
  )(TypedRecord.apply)(TypedRecord.unapply)

  val csvRowCodec: RowCodec[TypedRecord] =
    RowCodec.caseCodec(0, 1, 2, 3, 4)(TypedRecord.apply)(TypedRecord.unapply)
}

class CsvDynamicTest extends PipelineSpec {
  val aNames: Seq[String] = Seq("Anna", "Alice", "Albrecht", "Amy", "Arlo", "Agnes")
  val bNames: Seq[String] = Seq("Bob", "Barbara", "Barry", "Betty", "Brody", "Bard")

  def dynamicTest[T: Coder](
    input: Seq[T],
    save: (SCollection[T], Path) => ClosedTap[_],
    read: (ScioContext, Path) => (SCollection[String], SCollection[String])
  ): Unit = {
    val tmpDir = Files.createTempDirectory("csv-dynamic-io-")
    runWithRealContext()(sc => save(sc.parallelize(input), tmpDir))
    verifyOutput(tmpDir, "0", "1")

    runWithRealContext() { sc =>
      val (lines0, lines1) = read(sc, tmpDir)
      lines0 should containInAnyOrder(aNames)
      lines1 should containInAnyOrder(bNames)
    }

    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  private def verifyOutput(path: Path, expected: String*): Unit = {
    val p = path
    val actual = Files
      .list(p)
      .iterator()
      .asScala
      .filterNot(_.toFile.getName.startsWith("."))
      .toSet
    actual shouldBe expected.map(p.resolve).toSet
  }

  it should "write with headers" in {
    implicit val codec: HeaderCodec[TypedRecord] = TypedRecord.cscHeaderCodec

    val input =
      Random.shuffle(
        aNames.map(n => TypedRecord(int = 0, string = n)) ++
          bNames.map(n => TypedRecord(int = 1, string = n))
      )

    dynamicTest[TypedRecord](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicCsvFile(tmpDir.toAbsolutePath.toString)(t => s"${t.int}"),
      (sc2, tmpDir) => {
        val lines0 = sc2.csvFile(s"$tmpDir/0/*.csv").map(_.string)
        val lines1 = sc2.csvFile(s"$tmpDir/1/*.csv").map(_.string)
        (lines0, lines1)
      }
    )
  }

  it should "write skipping headers" in {
    implicit val encoder: HeaderEncoder[TypedRecord] = TypedRecord.cscHeaderCodec
    implicit val decoder: RowDecoder[TypedRecord] = TypedRecord.csvRowCodec

    val writeConfig =
      CsvIO.WriteParam.DefaultCsvConfig.copy(header = CsvConfiguration.Header.None)
    val readConfig =
      CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfiguration.withoutHeader)

    val input: Seq[TypedRecord] =
      Random.shuffle(
        aNames.map(n => TypedRecord(int = 0, string = n)) ++
          bNames.map(n => TypedRecord(int = 1, string = n))
      )

    dynamicTest[TypedRecord](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicCsvFile(tmpDir.toAbsolutePath.toString, csvConfig = writeConfig)(t =>
          s"${t.int}"
        ),
      (sc2, tmpDir) => {
        val lines0 = sc2.csvFile(s"$tmpDir/0/*.csv", readConfig).map(_.string)
        val lines1 = sc2.csvFile(s"$tmpDir/1/*.csv", readConfig).map(_.string)
        (lines0, lines1)
      }
    )
  }

  it should "write without headers" in {
    // defaultHeaderEncoder requires only a RowCodec and skips header
    implicit val codec: RowCodec[TypedRecord] = TypedRecord.csvRowCodec

    val input = Random.shuffle(
      aNames.map(n => TypedRecord(int = 0, string = n)) ++
        bNames.map(n => TypedRecord(int = 1, string = n))
    )

    val readConfig =
      CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfiguration.withoutHeader)

    dynamicTest[TypedRecord](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicCsvFile(tmpDir.toAbsolutePath.toString)(t => s"${t.int}"),
      (sc2, tmpDir) => {
        val lines0 = sc2.csvFile(s"$tmpDir/0/*.csv", readConfig).map(_.string)
        val lines1 = sc2.csvFile(s"$tmpDir/1/*.csv", readConfig).map(_.string)
        (lines0, lines1)
      }
    )
  }
}
