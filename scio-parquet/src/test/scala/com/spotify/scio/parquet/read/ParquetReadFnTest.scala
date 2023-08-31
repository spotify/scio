/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.parquet.read

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.Account
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.testing.PipelineSpec
import org.apache.commons.io.FileUtils
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.io.api.Binary
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

case class Record(strField: String)

class ParquetReadFnTest extends PipelineSpec with BeforeAndAfterAll {
  private val testDir = Files.createTempDirectory("scio-test").toFile
  private val testMultiDir = new File(testDir, "multi")
  private val testSingleDir = new File(testDir, "single")
  private val typedRecords = (1 to 250).map(i => Record(i.toString)).toList
  private val avroRecords = (251 to 500).map(i =>
    Account
      .newBuilder()
      .setId(i)
      .setType(i.toString)
      .setName(i.toString)
      .setAmount(i.toDouble)
      .build
  )

  override def beforeAll(): Unit = {
    // Multiple row-groups
    val multiRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 16)
    // Single row-group
    val singleRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 1073741824)

    val sc = ScioContext()
    val typedData = sc.parallelize(typedRecords)
    val avroData = sc.parallelize(avroRecords)

    typedData.saveAsTypedParquetFile(s"$testMultiDir/typed", conf = multiRowGroupConf)
    typedData.saveAsTypedParquetFile(s"$testSingleDir/typed", conf = singleRowGroupConf)

    avroData.saveAsParquetAvroFile(s"$testMultiDir/avro", conf = multiRowGroupConf)
    avroData.saveAsParquetAvroFile(s"$testSingleDir/avro", conf = singleRowGroupConf)

    sc.run()
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  "Parquet ReadFn" should "read at file-level granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()

    sc
      .parallelize(listFiles(s"${testMultiDir.getAbsolutePath}/typed"))
      .readFiles(
        ParquetRead.readTyped[Record](conf = granularityConf)
      ) should containInAnyOrder(typedRecords)

    sc.run()
  }

  it should "read at file-level granularity for files with a single row group" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()

    sc
      .parallelize(listFiles(s"${testMultiDir.getAbsolutePath}/typed"))
      .readFiles(
        ParquetRead.readTyped[Record](conf = granularityConf)
      ) should containInAnyOrder(typedRecords)

    sc.run()
  }

  it should "read at row-group granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    sc
      .typedParquetFile[Record](
        path = s"${testMultiDir.getAbsolutePath}/typed",
        conf = granularityConf,
        suffix = ".parquet"
      ) should containInAnyOrder(typedRecords)

    sc.run()
  }

  it should "read at row-group granularity for files with a single row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    sc
      .typedParquetFile[Record](
        path = s"${testSingleDir.getAbsolutePath}/typed",
        conf = granularityConf,
        suffix = ".parquet"
      ) should containInAnyOrder(typedRecords)

    sc.run()
  }

  "readTyped" should "work with a predicate" in {
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/typed"))
      .readFiles(
        ParquetRead.readTyped[Record](
          FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1"))
        )
      ) should containSingleValue(Record("1"))

    sc.run()
  }

  it should "work with a predicate and projection fn" in {
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/typed"))
      .readFiles(
        ParquetRead.readTyped(
          (r: Record) => r.strField,
          FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1")),
          ParquetConfiguration.empty()
        )
      ) should containSingleValue("1")

    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readTyped(
        (r: Record) => r.strField,
        FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1")),
        ParquetConfiguration.empty()
      )
    )
  }

  "readAvroGenericRecordFiles" should "work with a projection but no projectionFn" in {
    val projection = Projection[Account](_.getId)
    val expectedOut: Seq[GenericRecord] = (251 to 300).map { i =>
      new GenericRecordBuilder(projection).set("id", i).build()
    }

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          predicate = Predicate[Account](_.getId <= 300)
        )
      ) should containInAnyOrder(expectedOut)

    sc.run()
  }

  it should "work with a projection and projectionFn" in {
    val projection = Projection[Account](_.getId)

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          _.get("id").toString.toInt,
          predicate = Predicate[Account](_.getId <= 300)
        )
      ) should containInAnyOrder(251 to 300)

    sc.run()
  }

  it should "work with a projection and projectionFn on files with multiple row groups" in {
    val projection = Projection[Account](_.getId)

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testMultiDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          _.get("id").toString.toInt,
          predicate = Predicate[Account](_.getId <= 300)
        )
      ) should containInAnyOrder(251 to 300)

    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvroGenericRecordFiles(
        Projection[Account](_.getId),
        _.get("int_field").toString.toInt,
        predicate = Predicate[Account](_.getId <= 300)
      )
    )
  }

  "readAvro" should "work without a projection or a projectionFn" in {
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvro[Account](
          predicate = Predicate[Account](_.getId == 300)
        )
      ) should containSingleValue(avroRecords.find(_.getId == 300).get)
    sc.run()
  }

  it should "work with a projection but not a projectionFn as long as excluded fields are nullable" in {
    val projection = Projection[Account](_.getId, _.getType, _.getAmount)

    val sc = ScioContext()
    val output = sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvro[Account](
          projection,
          Predicate[Account](_.getId == 300)
        )
      )

    output should haveSize(1)
    output should satisfy[Account](
      _.forall(a =>
        a.getId == 300 && a.getName == null && a.getType == "300" && a.getAmount == 300.0
      )
    )
    sc.run()
  }

  it should "work with a projection and a projectionFn" in {
    val projection = Projection[Account](_.getId)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testSingleDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvro(
          projection,
          (a: Account) => a.getId.toInt,
          Predicate[Account](_.getId == 300)
        )
      ) should containSingleValue(300)
    sc.run()
  }

  it should "work with a projection and a projectionFn on files with multiple row groups" in {
    val projection = Projection[Account](_.getId)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"${testMultiDir.getAbsolutePath}/avro"))
      .readFiles(
        ParquetRead.readAvro(
          projection,
          (tr: Account) => tr.getId.toInt,
          Predicate[Account](_.getId == 300)
        )
      ) should containSingleValue(300)
    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvro(
        Projection[Account](_.getId),
        (tr: Account) => tr.getId.toInt,
        Predicate[Account](_.getId == 300)
      )
    )
  }

  "ParquetReadConfiguration" should "default to using splittableDoFn only if RunnerV2 experiment is enabled" in {
    // Default to true if RunnerV2 is set and user hasn't configured SDF explicitly
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs("--experiments=use_runner_v2,another_experiment").create()
    ) shouldBe true

    // Default to false if RunnerV2 is not set
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs("--experiments=another_experiment").create()
    ) shouldBe false

    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs().create()
    ) shouldBe false

    // Respect user's configuration, if set
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> false),
      PipelineOptionsFactory.fromArgs("--experiments=use_runner_v2").create()
    ) shouldBe false

    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> true),
      PipelineOptionsFactory.fromArgs().create()
    ) shouldBe true
  }

  private def listFiles(dir: String): Seq[String] =
    Files
      .list(Paths.get(dir))
      .iterator()
      .asScala
      .map(_.toFile.toPath.toString)
      .toSeq
}
