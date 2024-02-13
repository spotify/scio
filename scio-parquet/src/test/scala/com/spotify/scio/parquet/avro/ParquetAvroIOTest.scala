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

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.testing._
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.{Conversions, Schema}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory.GenericDatumFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro._
import org.joda.time.{DateTime, DateTimeFieldType, Duration, Instant}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll => forAllCases, Table}
import org.typelevel.scalaccompat.annotation.unused

import java.io.File
import java.nio.file.Files
import scala.concurrent.duration._
import scala.util.chaining._

class ParquetAvroIOFileNamePolicyTest extends FileNamePolicySpec[TestRecord] {
  override val suffix: String = ".parquet"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[TestRecord] = {
    in.map(AvroUtils.newSpecificRecord)
      .saveAsParquetAvroFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[TestRecord]] = Seq(
    _.map(AvroUtils.newSpecificRecord).saveAsParquetAvroFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

object ParquetAvroIOTest {

  final class TestLogicalTypesGenericDatumFactory extends GenericDatumFactory {
    override def apply(writer: Schema): DatumWriter[GenericRecord] = {
      val data = new GenericData()
      data.addLogicalTypeConversion(new TimeConversions.TimestampConversion)
      data.addLogicalTypeConversion(new Conversions.DecimalConversion)
      new GenericDatumWriter[GenericRecord](writer, data)
    }
    override def apply(writer: Schema, reader: Schema): DatumReader[GenericRecord] = {
      val data = new GenericData()
      data.addLogicalTypeConversion(new TimeConversions.TimestampConversion)
      data.addLogicalTypeConversion(new Conversions.DecimalConversion)
      new GenericDatumReader[GenericRecord](writer, reader, data)
    }
  }

  final class TestLogicalTypesDataSupplier extends AvroDataSupplier {
    override def get(): GenericData = new GenericData()
      .tap(_.addLogicalTypeConversion(new TimeConversions.TimestampConversion))
      .tap(_.addLogicalTypeConversion(new Conversions.DecimalConversion))
  }

  class TestRecordProjection(@unused str: String)
}

class ParquetAvroIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {

  import ParquetAvroIOTest._

  private val testDir = Files.createTempDirectory("scio-test-").toFile
  private val genericRecords = (1 to 10).map(AvroUtils.newGenericRecord)
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)
  private val projection = Projection[TestRecord](_.getIntField)
  private val predicate = Predicate[TestRecord](_.getIntField <= 5)
  private val projectionRecords = (1 to 10).map { i =>
    TestRecord.newBuilder().setIntField(i).build()
  }

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(specificRecords).saveAsParquetAvroFile(testDir.getAbsolutePath)
    sc.run()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  private def createConfig(splittable: Boolean): Configuration = {
    val c = ParquetConfiguration.empty()
    c.set(ParquetReadConfiguration.UseSplittableDoFn, splittable.toString)
    c
  }

  private val readConfigs =
    Table(
      ("config", "description"),
      (() => createConfig(false), "legacy read"),
      (() => createConfig(true), "splittable"),
      (() => ParquetAvroIO.ReadParam.DefaultConfiguration, "default")
    )

  "ParquetAvroIO" should "work with specific records" in {
    forAllCases(readConfigs) { case (c, _) =>
      testTap(specificRecords)(_.saveAsParquetAvroFile(_))(".parquet")
      testJobTest(specificRecords)(ParquetAvroIO(_))(_.parquetAvroFile[TestRecord](_, conf = c()))(
        _.saveAsParquetAvroFile(_)
      )
    }
  }

  it should "read specific records with projection" in {
    forAllCases(readConfigs) { case (c, _) =>
      runWithRealContext() { sc =>
        val result = sc.parquetAvroFile[TestRecord](
          path = testDir.getAbsolutePath,
          projection = projection,
          suffix = ".parquet",
          conf = c()
        )
        result should containInAnyOrder(projectionRecords)
      }
    }
  }

  it should "read specific records with predicate" in {
    forAllCases(readConfigs) { case (c, _) =>
      runWithRealContext() { sc =>
        val result = sc.parquetAvroFile[TestRecord](
          path = testDir.getAbsolutePath,
          predicate = predicate,
          suffix = ".parquet",
          conf = c()
        )
        val expected = specificRecords.filter(_.getIntField <= 5)
        result should containInAnyOrder(expected)
      }
    }
  }

  it should "read specific records with projection and predicate" in {
    forAllCases(readConfigs) { case (c, _) =>
      runWithRealContext() { sc =>
        val result = sc.parquetAvroFile[TestRecord](
          path = testDir.getAbsolutePath,
          projection = projection,
          predicate = predicate,
          suffix = ".parquet",
          conf = c()
        )
        val expected = projectionRecords.filter(_.getIntField <= 5)
        result should containInAnyOrder(expected)
      }
    }
  }

  it should "write and read GenericRecords with logical types" in withTempDir { dir =>
    val schema = TestLogicalTypes.getClassSchema
    val records: Seq[GenericRecord] = (1 to 10).map { i =>
      TestLogicalTypes
        .newBuilder()
        .setTimestamp(DateTime.now())
        .setDecimal(BigDecimal(i).setScale(2).bigDecimal)
        .build()
    }
    val datumFactory = new TestLogicalTypesGenericDatumFactory
    implicit val coder: Coder[GenericRecord] = avroCoder(datumFactory, schema)

    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)

      runWithRealContext() { sc =>
        sc.parallelize(records)
          .saveAsParquetAvroFile(
            path = testCaseDir.getAbsolutePath,
            schema = schema,
            datumFactory = datumFactory
          )
      }

      val conf = ParquetConfiguration.ofNullable(readConf())
      conf.setClass(
        AvroWriteSupport.AVRO_DATA_SUPPLIER,
        classOf[TestLogicalTypesDataSupplier],
        classOf[AvroDataSupplier]
      )

      runWithRealContext() { sc =>
        val result = sc
          .parquetAvroGenericRecordFile(
            path = testCaseDir.getAbsolutePath,
            schema = schema,
            conf = conf,
            suffix = ".parquet",
            datumFactory = datumFactory
          )
        result should containInAnyOrder(records)
      }
    }
  }

  it should "write and read SpecificRecords with logical types" in withTempDir { dir =>
    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)
      val records = (1 to 10).map(_ =>
        TestLogicalTypes
          .newBuilder()
          .setTimestamp(DateTime.now())
          .setDecimal(BigDecimal.decimal(1.0).setScale(2).bigDecimal)
          .build()
      )

      runWithRealContext() { sc =>
        sc
          .parallelize(records)
          .saveAsParquetAvroFile(path = testCaseDir.getAbsolutePath)
      }

      runWithRealContext() { sc =>
        val result = sc
          .parquetAvroFile[TestLogicalTypes](
            path = testCaseDir.getAbsolutePath,
            conf = readConf(),
            suffix = ".parquet"
          )
        result should containInAnyOrder(records)
      }
    }
  }

  it should "read/write generic records" in withTempDir { dir =>
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)

    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)

      runWithRealContext() { sc =>
        sc.parallelize(genericRecords)
          .saveAsParquetAvroFile(
            testCaseDir.getAbsolutePath,
            numShards = 1,
            schema = AvroUtils.schema
          )
      }

      val files = testCaseDir.listFiles()
      files.map(_.isDirectory).length shouldBe 1

      runWithRealContext() { sc =>
        val result = sc.parquetAvroGenericRecordFile(
          path = testCaseDir.getAbsolutePath,
          schema = AvroUtils.schema,
          conf = readConf(),
          suffix = ".parquet"
        )
        result should containInAnyOrder(genericRecords)
      }
    }
  }

  it should "write windowed generic records to dynamic destinations" in withTempDir { dir =>
    // This test follows the same pattern as com.spotify.scio.io.dynamic.DynamicFileTest
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)
    val streamingOptions = PipelineOptionsFactory.fromArgs("--streaming=true").create()

    val filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
      windowed = (shardNumber: Int, numShards: Int, window: BoundedWindow, _: PaneInfo) => {
        val intervalWindow = window.asInstanceOf[IntervalWindow]
        val year = intervalWindow.start().get(DateTimeFieldType.year())
        val month = intervalWindow.start().get(DateTimeFieldType.monthOfYear())
        val day = intervalWindow.start().get(DateTimeFieldType.dayOfMonth())
        val hour = intervalWindow.start().get(DateTimeFieldType.hourOfDay())
        "y=%02d/m=%02d/d=%02d/h=%02d/part-%s-of-%s"
          .format(year, month, day, hour, shardNumber, numShards)
      }
    )

    runWithRealContext(streamingOptions) { sc =>
      sc
        .parallelize(genericRecords)
        // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
        // mysterious "Could not find proxy for val sc1" compiler error
        // take each records int value and multiply it by half hour, so we should have 2 records in each hour window
        .timestampBy(
          x =>
            Instant.ofEpochMilli(
              (30.minutes * x.get("long_field").asInstanceOf[Long]).toMillis - 1
            ),
          Duration.ZERO
        )
        .withFixedWindows(Duration.standardHours(1), Duration.ZERO, WindowOptions())
        .saveAsParquetAvroFile(
          dir.getAbsolutePath,
          numShards = 1,
          schema = AvroUtils.schema,
          filenamePolicySupplier = filenamePolicySupplier
        )
    }

    def recursiveListFiles(directory: File): List[File] = {
      val files = directory.listFiles()
      files.filter(!_.isDirectory).toList ++ files.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    val files = recursiveListFiles(dir)
    files.length shouldBe 5

    (1 to 10)
      .sliding(2, 2)
      .zipWithIndex
      .foreach { case (window, idx) =>
        val filename = f"y=1970/m=01/d=01/h=$idx%02d/part-0-of-1.parquet"
        val records = window.map(AvroUtils.newGenericRecord)
        val tap = ParquetGenericRecordTap(s"$dir/$filename", AvroUtils.schema)
        tap.value.toList should contain theSameElementsAs records
      }
  }

  it should "write generic records to dynamic destinations" in withTempDir { dir =>
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)

    val filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
      unwindowed =
        (shardNumber: Int, numShards: Int) => s"part-$shardNumber-of-$numShards-with-custom-naming"
    )

    runWithRealContext() { sc =>
      sc.parallelize(genericRecords)
        .saveAsParquetAvroFile(
          dir.getAbsolutePath,
          numShards = 1,
          schema = AvroUtils.schema,
          filenamePolicySupplier = filenamePolicySupplier
        )
    }

    val files = dir.listFiles()
    files.length shouldBe 1
    files.head.getAbsolutePath should include("part-0-of-1-with-custom-naming.parquet")

    val tap = ParquetGenericRecordTap(files.head.getAbsolutePath, AvroUtils.schema)
    tap.value.toList should contain theSameElementsAs genericRecords
  }

  it should "throw exception when filename functions not correctly defined for un-windows dynamic destinations" in withTempDir {
    dir =>
      implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)

      val filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
        windowed = (_, _, _, _) => "test for exception handling"
      )

      val e = the[PipelineExecutionException] thrownBy {
        runWithRealContext() { sc =>
          sc.parallelize(genericRecords)
            .saveAsParquetAvroFile(
              dir.getAbsolutePath,
              numShards = 1,
              schema = AvroUtils.schema,
              filenamePolicySupplier = filenamePolicySupplier
            )
        }
      }
      e.getCause shouldBe a[NotImplementedError]
  }

  it should "throw exception when filename functions not correctly defined for windowed dynamic destinations" in withTempDir {
    dir =>
      implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)

      val filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
        unwindowed = (_, _) => "test for exception handling"
      )

      val e = the[PipelineExecutionException] thrownBy {
        runWithRealContext() { sc =>
          sc.parallelize(genericRecords)
            .timestampBy(
              x => new Instant(x.get("int_field").asInstanceOf[Int] * 1800000L),
              Duration.ZERO
            )
            .withFixedWindows(Duration.standardHours(1), Duration.ZERO, WindowOptions())
            .saveAsParquetAvroFile(
              dir.getAbsolutePath,
              numShards = 1,
              schema = AvroUtils.schema,
              filenamePolicySupplier = filenamePolicySupplier
            )
        }
      }
      e.getCause shouldBe a[NotImplementedError]
  }

  it should "apply map functions to test input" in {
    val expected = specificRecords.map(_.getIntField.toString)
    JobTest[ParquetTestJob.type]
      .args("--input=input", "--output=output")
      .input(ParquetAvroIO[TestRecord]("input"), specificRecords)
      .output(TextIO("output"))(_ should containInAnyOrder(expected))
      .run()
  }

  "ParquetTap" should "use projection schema" in {
    ParquetGenericRecordTap(
      testDir.getAbsolutePath,
      TestRecord.getClassSchema,
      params = ParquetAvroIO.ReadParam(projection, suffix = ".parquet")
    ).value.foreach { gr =>
      gr.get("int_field") should not be null
      gr.get("string_field") shouldBe null
    }
  }
}

object ParquetTestJob {
  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    sc
      .parquetAvroFile[TestRecord](
        args("input"),
        projection = Projection[TestRecord](_.getIntField)
      )
      .map(_.getIntField)
      .saveAsTextFile(args("output"))
    sc.run().waitUntilDone()
  }
}
