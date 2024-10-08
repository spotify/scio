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
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, FileNamePolicySpec, ScioIOTest, TapSpec, TextIO}
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.testing._
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.avro.data.TimeConversions
import org.apache.avro.{Conversions, Schema}
import org.apache.avro.generic.{
  GenericData,
  GenericDatumReader,
  GenericDatumWriter,
  GenericRecord,
  GenericRecordBuilder
}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, AvroWriteSupport}
import org.joda.time.{DateTimeFieldType, Duration, Instant}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll => forAllCases, Table}
import org.typelevel.scalaccompat.annotation.unused

import java.nio.file.Files

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

class ParquetAvroIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {
  private val testDir = Files.createTempDirectory("scio-test-").toFile
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

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

  it should "work with specific records" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)

    forAllCases(readConfigs) { case (c, _) =>
      testTap(xs)(_.saveAsParquetAvroFile(_))(".parquet")
      testJobTest(xs)(ParquetAvroIO(_))(
        _.parquetAvroFile[TestRecord](_, conf = c()).map(identity)
      )(_.saveAsParquetAvroFile(_))
    }
  }

  it should "read specific records with projection" in {
    forAllCases(readConfigs) { case (c, _) =>
      val sc = ScioContext()
      val projection = Projection[TestRecord](_.getIntField)
      val data = sc.parquetAvroFile[TestRecord](
        path = testDir.getAbsolutePath,
        projection = projection,
        suffix = ".parquet",
        conf = c()
      )

      data.map(_.getIntField.toInt) should containInAnyOrder(1 to 10)
      data.map(identity) should forAll[TestRecord] { r =>
        r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null && r.getArrayField
          .size() == 0
      }
      sc.run()
    }
  }

  it should "read specific records with predicate" in {
    forAllCases(readConfigs) { case (c, _) =>
      val sc = ScioContext()
      val predicate = Predicate[TestRecord](_.getIntField <= 5)
      val data = sc.parquetAvroFile[TestRecord](
        path = testDir.getAbsolutePath,
        predicate = predicate,
        suffix = ".parquet",
        conf = c()
      )
      val expected = specificRecords.filter(_.getIntField <= 5)
      data.map(identity) should containInAnyOrder(expected)
      sc.run()
    }
  }

  it should "read specific records with projection and predicate" in {
    forAllCases(readConfigs) { case (c, _) =>
      val sc = ScioContext()
      val projection = Projection[TestRecord](_.getIntField)
      val predicate = Predicate[TestRecord](_.getIntField <= 5)
      val data = sc.parquetAvroFile[TestRecord](
        path = testDir.getAbsolutePath,
        projection = projection,
        predicate = predicate,
        suffix = ".parquet",
        conf = c()
      )
      data.map(_.getIntField.toInt) should containInAnyOrder(1 to 5)
      data.map(identity) should forAll[TestRecord] { r =>
        r.getLongField == null &&
        r.getFloatField == null &&
        r.getDoubleField == null &&
        r.getBooleanField == null &&
        r.getStringField == null &&
        r.getArrayField.size() == 0
      }
      sc.run()
    }
  }

  it should "write and read SpecificRecords with default logical types" in withTempDir { dir =>
    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)
      val records = (1 to 10).map(_ =>
        TestLogicalTypes
          .newBuilder()
          .setTimestamp(java.time.Instant.now())
          .setDecimal(BigDecimal.decimal(1.0).setScale(2).bigDecimal)
          .build()
      )

      val sc1 = ScioContext()
      sc1
        .parallelize(records)
        .saveAsParquetAvroFile(path = testCaseDir.getAbsolutePath)
      sc1.run()

      val sc2 = ScioContext()
      sc2
        .parquetAvroFile[TestLogicalTypes](
          path = testCaseDir.getAbsolutePath,
          conf = readConf(),
          suffix = ".parquet"
        )
        .map(identity) should containInAnyOrder(records)

      sc2.run()
    }
  }

  it should "write and read GenericRecords with default logical types" in withTempDir { dir =>
    // forAllCases(readConfigs) { case (readConf, testCase) =>
    // val testCaseDir = new File(dir, testCase)
    val records: Seq[GenericRecord] = (1 to 10).map { _ =>
      new GenericRecordBuilder(TestLogicalTypes.getClassSchema)
        .set("timestamp", java.time.Instant.now())
        .set("decimal", BigDecimal.decimal(1.0).setScale(2).bigDecimal)
        .build()
    }

    val logicalTypeDatumFactory = new AvroDatumFactory(classOf[GenericRecord]) {
      override def apply(writer: Schema, reader: Schema): DatumReader[GenericRecord] = {
        val data = new GenericData()
        data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion)
        data.addLogicalTypeConversion(new Conversions.DecimalConversion)
        new GenericDatumReader[GenericRecord](writer, reader, data)
      }

      override def apply(writer: Schema): DatumWriter[GenericRecord] = {
        val data = new GenericData()
        data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion)
        data.addLogicalTypeConversion(new Conversions.DecimalConversion)
        new GenericDatumWriter[GenericRecord](writer, data)
      }
    }

    implicit val coder: Coder[GenericRecord] =
      avroCoder(logicalTypeDatumFactory, TestLogicalTypes.getClassSchema)

    val sc1 = ScioContext()
    val c1 = new Configuration()
    AvroWriteSupport.setAvroDataSupplier(c1, classOf[LogicalTypeDataSupplier])
    sc1
      .parallelize(records)
      .saveAsParquetAvroFile(
        path = dir.getAbsolutePath,
        schema = TestLogicalTypes.getClassSchema,
        conf = c1
      )
    sc1.run()

    val sc2 = ScioContext()
    val c2 = new Configuration()
    AvroReadSupport.setAvroDataSupplier(c2, classOf[LogicalTypeDataSupplier])
    sc2
      .parquetAvroFile[GenericRecord](
        path = dir.getAbsolutePath,
        projection = TestLogicalTypes.getClassSchema,
        conf = c2,
        suffix = ".parquet"
      )
      .map(identity) should containInAnyOrder(records)

    sc2.run()
    // }
  }

  it should "write and read SpecificRecords with custom logical types" in withTempDir { dir =>
    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)
      val records =
        (1 to 10).map(_ =>
          TestLogicalTypes
            .newBuilder()
            .setTimestamp(java.time.Instant.now())
            .setDecimal(BigDecimal.decimal(1.0).setScale(2).bigDecimal)
            .build()
        )

      val sc1 = ScioContext()
      sc1
        .parallelize(records)
        .saveAsParquetAvroFile(
          path = testCaseDir.getAbsolutePath
        )
      sc1.run()

      val sc2 = ScioContext()
      sc2
        .parquetAvroFile[TestLogicalTypes](
          path = testCaseDir.getAbsolutePath,
          conf = readConf(),
          suffix = ".parquet"
        )
        .map(identity) should containInAnyOrder(records)

      sc2.run()
      ()
    }
  }

  it should "read with incomplete projection" in withTempDir { dir =>
    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)
      val sc1 = ScioContext()
      val nestedRecords =
        (1 to 10).map(x => new Account(x, x.toString, x.toString, x.toDouble, AccountStatus.Active))
      sc1
        .parallelize(nestedRecords)
        .saveAsParquetAvroFile(testCaseDir.getAbsolutePath)
      sc1.run()

      val sc2 = ScioContext()
      val projection = Projection[Account](_.getName)
      val data = sc2.parquetAvroFile[Account](
        path = testCaseDir.getAbsolutePath,
        projection = projection,
        conf = readConf(),
        suffix = ".parquet"
      )
      val expected = nestedRecords.map(_.getName.toString)
      data.map(_.getName.toString) should containInAnyOrder(expected)
      data.flatMap(a => Some(a.getName.toString)) should containInAnyOrder(expected)
      sc2.run()
    }
  }

  it should "read/write generic records" in withTempDir { dir =>
    forAllCases(readConfigs) { case (readConf, testCase) =>
      val testCaseDir = new File(dir, testCase)
      val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
      val sc1 = ScioContext()
      implicit val coder = avroGenericRecordCoder(AvroUtils.schema)
      sc1
        .parallelize(genericRecords)
        .saveAsParquetAvroFile(
          testCaseDir.getAbsolutePath,
          numShards = 1,
          schema = AvroUtils.schema
        )
      sc1.run()

      val files = testCaseDir.listFiles()
      files.map(_.isDirectory).length shouldBe 1

      val sc2 = ScioContext()
      val data: SCollection[GenericRecord] = sc2.parquetAvroFile[GenericRecord](
        path = testCaseDir.getAbsolutePath,
        projection = AvroUtils.schema,
        conf = readConf(),
        suffix = ".parquet"
      )
      data should containInAnyOrder(genericRecords)
      sc2.run()
    }
  }

  class TestRecordProjection(@unused str: String)

  "tap" should "use projection schema and GenericDataSupplier" in {
    val schema = new Schema.Parser().parse(
      """
        |{
        |"type":"record",
        |"name":"TestRecordProjection",
        |"namespace":"com.spotify.scio.parquet.avro.ParquetAvroIOTest$",
        |"fields":[{"name":"int_field","type":["null", "int"]}]}
        |""".stripMargin
    )

    implicit val coder = avroGenericRecordCoder(schema)

    ParquetAvroTap(
      s"${testDir.toPath}",
      ParquetAvroIO.ReadParam(identity[GenericRecord], schema, suffix = "*.parquet")
    ).value.foreach { gr =>
      gr.hasField("int_field") shouldBe true
      gr.hasField("string_field") shouldBe false
    }
  }

  it should "write windowed generic records to dynamic destinations" in withTempDir { dir =>
    // This test follows the same pattern as com.spotify.scio.io.dynamic.DynamicFileTest
    val genericRecords = (0 until 10).map(AvroUtils.newGenericRecord)
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    implicit val coder = avroGenericRecordCoder(AvroUtils.schema)
    sc1
      .parallelize(genericRecords)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      // take each records int value and multiply it by half hour, so we should have 2 records in each hour window
      .timestampBy(x => new Instant(x.get("int_field").asInstanceOf[Int] * 1800000L), Duration.ZERO)
      .withFixedWindows(Duration.standardHours(1), Duration.ZERO, WindowOptions())
      .saveAsParquetAvroFile(
        dir.getAbsolutePath,
        numShards = 1,
        schema = AvroUtils.schema,
        filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
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
        identity[GenericRecord],
        AvroUtils.schema,
        null
      )

    (0 until 10)
      .sliding(2, 2)
      .zipWithIndex
      .map { case (window, idx) =>
        (
          f"y=1970/m=01/d=01/h=$idx%02d/part-0-of-1.parquet",
          window.map(AvroUtils.newGenericRecord)
        )
      }
      .foreach {
        case (filename, records) => {
          val tap = ParquetAvroTap(s"$dir/$filename", params)
          tap.value.toList should contain theSameElementsAs records
        }
      }
  }

  it should "write generic records to dynamic destinations" in withTempDir { dir =>
    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    val sc = ScioContext()
    implicit val coder = avroGenericRecordCoder(AvroUtils.schema)
    sc.parallelize(genericRecords)
      .saveAsParquetAvroFile(
        dir.getAbsolutePath,
        numShards = 1,
        schema = AvroUtils.schema,
        filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
          unwindowed = (shardNumber: Int, numShards: Int) =>
            s"part-$shardNumber-of-$numShards-with-custom-naming"
        )
      )
    sc.run()

    val files = dir.listFiles()
    files.length shouldBe 1
    files.head.getAbsolutePath should include("part-0-of-1-with-custom-naming.parquet")

    val params = ParquetAvroIO.ReadParam[GenericRecord, GenericRecord](
      identity[GenericRecord],
      AvroUtils.schema,
      null
    )

    val tap = ParquetAvroTap(files.head.getAbsolutePath, params)

    tap.value.toList should contain theSameElementsAs genericRecords
  }

  it should "throw exception when filename functions not correctly defined for dynamic destinations" in withTempDir {
    dir =>
      val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
      implicit val coder = avroGenericRecordCoder(AvroUtils.schema)

      an[NotImplementedError] should be thrownBy {
        val sc = ScioContext()
        sc.parallelize(genericRecords)
          .saveAsParquetAvroFile(
            dir.getAbsolutePath,
            numShards = 1,
            schema = AvroUtils.schema,
            filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
              windowed = (_, _, _, _) => "test for exception handling"
            )
          )
        try {
          sc.run()
        } catch {
          case e: PipelineExecutionException =>
            throw e.getCause
        }
      }

      val e = the[PipelineExecutionException] thrownBy {
        val sc = ScioContext()
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
            filenamePolicySupplier = FilenamePolicySupplier.filenamePolicySupplierOf(
              unwindowed = (_, _) => "test for exception handling"
            )
          )
        sc.run()
      }
      e.getCause shouldBe a[NotImplementedError]
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

class LogicalTypeDataSupplier extends AvroDataSupplier {
  override def get(): GenericData = {
    val data = new GenericData()
    data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion)
    data.addLogicalTypeConversion(new Conversions.DecimalConversion)
    data
  }
}
