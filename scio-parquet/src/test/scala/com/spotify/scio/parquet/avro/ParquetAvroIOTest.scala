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

package com.spotify.scio.parquet.avro

import java.io.File
import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.avro._
import com.spotify.scio.io.{TapSpec, TextIO}
import com.spotify.scio.testing._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.commons.io.FileUtils
import org.joda.time.{DateTimeFieldType, Duration, Instant}
import org.scalatest.BeforeAndAfterAll

class ParquetAvroIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {
  private val dir = tmpDir
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(specificRecords)
      .saveAsParquetAvroFile(dir.toString)
    sc.run()
    ()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(dir)

  "ParquetAvroIO" should "work with specific records" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(_.saveAsParquetAvroFile(_))(".parquet")
    testJobTest(xs)(ParquetAvroIO(_))(_.parquetAvroFile[TestRecord](_).map(identity))(
      _.saveAsParquetAvroFile(_)
    )
  }

  it should "read specific records with projection" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val data = sc
      .parquetAvroFile[TestRecord](s"$dir/*.parquet", projection = projection)
    data.map(_.getIntField.toInt) should containInAnyOrder(1 to 10)
    data.map(identity) should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
      r.getBooleanField == null && r.getStringField == null && r.getArrayField
        .size() == 0
    }
    sc.run()
    ()
  }

  it should "read specific records with predicate" in {
    val sc = ScioContext()
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data =
      sc.parquetAvroFile[TestRecord](s"$dir/*.parquet", predicate = predicate)
    data.map(identity) should containInAnyOrder(specificRecords.filter(_.getIntField <= 5))
    sc.run()
    ()
  }

  it should "read specific records with projection and predicate" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data =
      sc.parquetAvroFile[TestRecord](s"$dir/*.parquet", projection, predicate)
    data.map(_.getIntField.toInt) should containInAnyOrder(1 to 5)
    data.map(identity) should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
      r.getBooleanField == null && r.getStringField == null && r.getArrayField
        .size() == 0
    }
    sc.run()
    ()
  }

  it should "read with incomplete projection" in {
    val dir = tmpDir

    val sc1 = ScioContext()
    val nestedRecords =
      (1 to 10).map(x => new Account(x, x.toString, x.toString, x.toDouble, AccountStatus.Active))
    sc1
      .parallelize(nestedRecords)
      .saveAsParquetAvroFile(dir.toString)
    sc1.run()

    val sc2 = ScioContext()
    val projection = Projection[Account](_.getName)
    val data =
      sc2.parquetAvroFile[Account](s"$dir/*.parquet", projection = projection)
    val expected = nestedRecords.map(_.getName.toString)
    data.map(_.getName.toString) should containInAnyOrder(expected)
    data.flatMap(a => Some(a.getName.toString)) should containInAnyOrder(expected)
    sc2.run()

    FileUtils.deleteDirectory(dir)
  }

  it should "read/write generic records" in {
    val dir = tmpDir

    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    val sc1 = ScioContext()
    implicit val coder = Coder.avroGenericRecordCoder(AvroUtils.schema)
    sc1
      .parallelize(genericRecords)
      .saveAsParquetAvroFile(dir.toString, numShards = 1, schema = AvroUtils.schema)
    sc1.run()

    val files = dir.listFiles()
    files.length shouldBe 1

    val sc2 = ScioContext()
    val data: SCollection[GenericRecord] =
      sc2.parquetAvroFile[GenericRecord](s"$dir/*.parquet", AvroUtils.schema)
    data should containInAnyOrder(genericRecords)
    sc2.run()

    FileUtils.deleteDirectory(dir)
  }

  it should "write windowed generic records to dynamic destinations" in {
    // This test follows the same pattern as com.spotify.scio.io.dynamic.DynamicFileTest

    val dir = tmpDir

    val genericRecords = (0 until 10).map(AvroUtils.newGenericRecord)
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    implicit val coder = Coder.avroGenericRecordCoder(AvroUtils.schema)
    sc1
      .parallelize(genericRecords)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      // take each records int value and multiply it by half hour, so we should have 2 records in each hour window
      .timestampBy(x => new Instant(x.get("int_field").asInstanceOf[Int] * 1800000L), Duration.ZERO)
      .withFixedWindows(Duration.standardHours(1), Duration.ZERO, WindowOptions())
      .saveAsDynamicParquetAvroFile(
        dir.toString,
        Left { (shardNumber: Int, numShards: Int, window: BoundedWindow, _: PaneInfo) =>
          val intervalWindow = window.asInstanceOf[IntervalWindow]
          val year = intervalWindow.start().get(DateTimeFieldType.year())
          val month = intervalWindow.start().get(DateTimeFieldType.monthOfYear())
          val day = intervalWindow.start().get(DateTimeFieldType.dayOfMonth())
          val hour = intervalWindow.start().get(DateTimeFieldType.hourOfDay())
          "y=%02d/m=%02d/d=%02d/h=%02d/part-%s-of-%s"
            .format(year, month, day, hour, shardNumber, numShards)
        },
        numShards = 1,
        schema = AvroUtils.schema
      )
    sc1.run()

    def recursiveListFiles(directory: File): List[File] = {
      val files = directory.listFiles()
      files.filter(!_.isDirectory).toList ++ files.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    val files = recursiveListFiles(dir)
    files.length shouldBe 5

    val params =
      ParquetAvroIO.ReadParam[GenericRecord, GenericRecord](
        identity[GenericRecord] _,
        AvroUtils.schema,
        null
      )

    (0 until 10)
      .sliding(2, 2)
      .zipWithIndex
      .map(t =>
        (
          "y=1970/m=01/d=01/h=%02d/part-0-of-1.parquet".format(t._2),
          t._1.map(AvroUtils.newGenericRecord)
        )
      )
      .foreach {
        case (filename, records) => {
          val tap = ParquetAvroTap(s"$dir/$filename", params)
          tap.value.toList should contain theSameElementsAs records
        }
      }

    FileUtils.deleteDirectory(dir)
  }

  it should "write generic records to dynamic destinations" in {
    val dir = tmpDir

    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    val sc = ScioContext()
    implicit val coder = Coder.avroGenericRecordCoder(AvroUtils.schema)
    sc.parallelize(genericRecords)
      .saveAsDynamicParquetAvroFile(
        dir.toString,
        Right((shardNumber: Int, numShards: Int) =>
          "part-%s-of-%s-with-custom-naming".format(shardNumber, numShards)
        ),
        numShards = 1,
        schema = AvroUtils.schema
      )
    sc.run()

    val files = dir.listFiles()
    files.length shouldBe 1
    files.head.getAbsolutePath should include("part-0-of-1-with-custom-naming.parquet")

    val params =
      ParquetAvroIO.ReadParam[GenericRecord, GenericRecord](
        identity[GenericRecord] _,
        AvroUtils.schema,
        null
      )
    val tap = ParquetAvroTap(files.head.getAbsolutePath, params)
    tap.value.toList should contain theSameElementsAs genericRecords

    FileUtils.deleteDirectory(dir)
  }

  it should "throw exception when filename functions not correctly defined for dynamic destinations" in {
    val dir = tmpDir

    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    implicit val coder = Coder.avroGenericRecordCoder(AvroUtils.schema)

    an[NotImplementedError] should be thrownBy {
      val sc = ScioContext()
      sc.parallelize(genericRecords)
        .saveAsDynamicParquetAvroFile(
          dir.toString,
          Left((_, _, _, _) => "test for exception handling"),
          numShards = 1,
          schema = AvroUtils.schema
        )
      sc.run()
    }

    an[NotImplementedError] should be thrownBy {
      val sc = ScioContext()
      sc.parallelize(genericRecords)
        .timestampBy(
          x => new Instant(x.get("int_field").asInstanceOf[Int] * 1800000L),
          Duration.ZERO
        )
        .withFixedWindows(Duration.standardHours(1), Duration.ZERO, WindowOptions())
        .saveAsDynamicParquetAvroFile(
          dir.toString,
          Right((_, _) => "test for exception handling"),
          numShards = 1,
          schema = AvroUtils.schema
        )
      sc.run()
    }

  }

  it should "apply map functions to test input" in {
    JobTest[ParquetTestJob.type]
      .args("--input=input", "--output=output")
      .input(
        ParquetAvroIO[Account]("input"),
        List(Account.newBuilder().setId(1).setName("foo").setType("bar").setAmount(2.0).build())
      )
      .output(TextIO("output"))(_ should containSingleValue(("foo", 2.0).toString))
      .run()
  }
}

object ParquetTestJob {
  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    sc
      .parquetAvroFile[Account](
        args("input"),
        projection = Projection[Account](_.getName, _.getAmount)
      )
      .map(a => (a.getName.toString, a.getAmount))
      .saveAsTextFile(args("output"))
    sc.run().waitUntilDone()
  }
}
