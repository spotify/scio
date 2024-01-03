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
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Random

case class TypedRecord(
  int: Int,
  long: Long,
  float: Float,
  bool: Boolean,
  string: String
)

trait CsvDynamicTest extends PipelineSpec {
  val aNames: Seq[String] = Seq("Anna", "Alice", "Albrecht", "Amy", "Arlo", "Agnes")
  val bNames: Seq[String] = Seq("Bob", "Barbara", "Barry", "Betty", "Brody", "Bard")

  def dynamicTest[T: Coder](
    input: Seq[T],
    save: (SCollection[T], Path) => ClosedTap[_],
    read: (ScioContext, Path) => (SCollection[String], SCollection[String])
  ): Unit = {
    val tmpDir = Files.createTempDirectory("csv-dynamic-io-")
    val sc = ScioContext()
    save(sc.parallelize(input), tmpDir)
    sc.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val (lines0, lines1) = read(sc2, tmpDir)

    lines0 should containInAnyOrder(aNames)
    lines1 should containInAnyOrder(bNames)
    sc2.run()
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
}

class TypedCsvDynamicTest extends CsvDynamicTest {
  import kantan.csv._
  import com.spotify.scio.extra.csv._

  it should "write with headers" in {
    implicit val encoder: HeaderEncoder[TypedRecord] =
      HeaderEncoder.caseEncoder(
        "int",
        "long",
        "float",
        "bool",
        "string"
      )(TypedRecord.unapply)
    implicit val decoder: HeaderDecoder[TypedRecord] = HeaderDecoder.decoder(
      "int",
      "long",
      "float",
      "bool",
      "string"
    )(TypedRecord.apply)

    def typedRec(int: Int, name: String): TypedRecord =
      TypedRecord(int, 0L, 0f, false, name)
    val input: Seq[TypedRecord] =
      Random.shuffle(aNames.map(n => typedRec(0, n)) ++ bNames.map(n => typedRec(1, n)))

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

  it should "write without headers" in {
    implicit val encoder: HeaderEncoder[TypedRecord] =
      HeaderEncoder.caseEncoder(
        "int",
        "long",
        "float",
        "bool",
        "string"
      )(TypedRecord.unapply)
    implicit val decoder: RowDecoder[TypedRecord] = RowDecoder.ordered {
      (int: Int, long: Long, float: Float, bool: Boolean, string: String) =>
        TypedRecord(int, long, float, bool, string)
    }
    val writeConfig =
      CsvIO.WriteParam.DefaultCsvConfig.copy(header = CsvConfiguration.Header.None)
    val readConfig =
      CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfiguration.withoutHeader)

    def typedRec(int: Int, name: String): TypedRecord =
      TypedRecord(int, 0L, 0f, false, name)
    val input: Seq[TypedRecord] =
      Random.shuffle(aNames.map(n => typedRec(0, n)) ++ bNames.map(n => typedRec(1, n)))

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

  it should "write with a row encoder" in {
    implicit val encoder: RowEncoder[TypedRecord] =
      RowEncoder.encoder(0, 1, 2, 3, 4)((t: TypedRecord) =>
        (t.int, t.long, t.float, t.bool, t.string)
      )
    implicit val decoder: RowDecoder[TypedRecord] = RowDecoder.ordered {
      (int: Int, long: Long, float: Float, bool: Boolean, string: String) =>
        TypedRecord(int, long, float, bool, string)
    }
    val writeConfig =
      CsvIO.WriteParam.DefaultCsvConfig.copy(header = CsvConfiguration.Header.None)
    val readConfig =
      CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfiguration.withoutHeader)

    def typedRec(int: Int, name: String): TypedRecord =
      TypedRecord(int, 0L, 0f, false, name)
    val input: Seq[TypedRecord] =
      Random.shuffle(aNames.map(n => typedRec(0, n)) ++ bNames.map(n => typedRec(1, n)))

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
}
