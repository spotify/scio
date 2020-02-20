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
package com.spotify.scio.extra.csv

import java.io.File
import java.nio.charset.StandardCharsets

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.csv.CsvIOTest.TestTuple
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.values.SCollection
import kantan.csv.{CsvConfiguration, HeaderDecoder, HeaderEncoder, RowDecoder, RowEncoder}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._
object CsvIOTest {
  case class TestTuple(a: Int, string: String)
}

class CsvIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterEach {

  var dir: File = _
  override protected def beforeEach(): Unit = dir = tmpDir
  override protected def afterEach(): Unit = FileUtils.deleteDirectory(dir)

  "read csv" should "read strings" in {

    val csv = """header1
                |data1
                |data2
              """

    implicit val decoder: HeaderDecoder[String] =
      HeaderDecoder.decoder("header1")((str: String) => str)

    parse(csv) should containInAnyOrder(Seq("data1", "data2"))
  }

  it should "read tuples" in {
    val csv =
      """
        |numericValue, stringValue
        |1,test1
        |2,test2
        |"""

    implicit val decoder: HeaderDecoder[TestTuple] =
      HeaderDecoder.decoder("numericValue", "stringValue")(TestTuple.apply)

    parse(csv) should containInAnyOrder(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      )
    )
  }

  it should "read tuples with reversed headers" in {
    val csv =
      """
        |stringValue, numericValue
        |test1,1
        |test2,2
        |"""

    implicit val decoder: HeaderDecoder[TestTuple] =
      HeaderDecoder.decoder("numericValue", "stringValue")(TestTuple.apply)

    parse(csv) should containInAnyOrder(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      )
    )
  }

  it should "read ordered items without a header" in {
    val csv =
      """
        |test1,1
        |test2,2
        |"""

    implicit val decoder: RowDecoder[TestTuple] = RowDecoder.ordered { (string: String, i: Int) =>
      TestTuple(i, string)
    }

    parse(csv) should containInAnyOrder(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      )
    )
  }

  "CsvIO.Write" should "write with headers" in {
    implicit val encoder: HeaderEncoder[TestTuple] =
      HeaderEncoder.caseEncoder("intValue", "stringValue")(TestTuple.unapply)

    val csvLines = writeAsCsvAndReadLines(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      )
    )
    csvLines.head should be("intValue,stringValue")
    csvLines.tail should contain allElementsOf Seq(
      "1,test1",
      "2,test2"
    )
  }

  it should "write without headers" in {
    implicit val encoder: HeaderEncoder[TestTuple] =
      HeaderEncoder.caseEncoder("intValue", "stringValue")(TestTuple.unapply)
    val noHeaderConfig = CsvIO.DEFAULT_CSV_CONFIG.copy(header = CsvConfiguration.Header.None)

    val csvLines = writeAsCsvAndReadLines(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      ),
      CsvIO.WriteParam(csvConfiguration = noHeaderConfig)
    )
    csvLines should contain allElementsOf Seq(
      "1,test1",
      "2,test2"
    )
  }

  it should "write with a row encoder" in {
    implicit val encoder: RowEncoder[TestTuple] =
      RowEncoder.encoder(0, 1)((tup: TestTuple) => (tup.a, tup.string))
    val noHeaderConfig = CsvIO.DEFAULT_CSV_CONFIG.copy(header = CsvConfiguration.Header.None)

    val csvLines = writeAsCsvAndReadLines(
      Seq(
        TestTuple(1, "test1"),
        TestTuple(2, "test2")
      ),
      CsvIO.WriteParam(csvConfiguration = noHeaderConfig)
    )
    csvLines should contain allElementsOf Seq(
      "1,test1",
      "2,test2"
    )
  }

  private def writeAsCsvAndReadLines[T: HeaderEncoder: Coder](
    items: Seq[T],
    params: CsvIO.WriteParam = CsvIO.DEFAULT_WRITE_PARAMS
  ): List[String] = {
    val sc = ScioContext()

    sc.parallelize(items)
      .saveAsCsvFile(dir.getPath, params)

    sc.run().waitUntilFinish()

    val file = dir.listFiles((_, fileName) => fileName.endsWith("csv")).head

    FileUtils.readLines(file, StandardCharsets.UTF_8).asScala.toList
  }

  private def parse[T: HeaderDecoder: Coder](csv: String): SCollection[T] = {
    val file = new File(dir, "source.csv")
    FileUtils.write(file, csv, StandardCharsets.UTF_8)
    ScioContext().csvFile(file.getAbsolutePath)
  }
}
