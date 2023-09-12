/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.parquet.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.{avroGenericRecordCoder, AvroUtils, TestRecord}
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.ParquetRead
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, Tables}

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class ParquetReadAvroTest
    extends PipelineSpec
    with BeforeAndAfterAll
    with TableDrivenPropertyChecks {
  private val options = PipelineOptionsFactory.create()
  private val testDir = Files.createTempDirectory("scio-test").toFile
  // Multiple row-groups
  private val multiRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 16)
  private val testMultiDir = new File(testDir, "multi")
  // Single row-group
  private val singleRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 1073741824)
  private val testSingleDir = new File(testDir, "single")

  private val rowGroups = Table(
    "rew group",
    testMultiDir,
    testSingleDir
  )

  private val avroRecords = (251 to 500).map(AvroUtils.newSpecificRecord)
  private val projection = Projection[TestRecord](_.getIntField)
  private val projectedRecords = (251 to 500).map { i =>
    TestRecord
      .newBuilder()
      .setIntField(i)
      .build
  }

  private val predicate = Predicate[TestRecord](_.getIntField <= 300)
  private val filteredRecords = avroRecords.filter(_.getIntField <= 300)
  private val filteredProjectedRecords = projectedRecords.filter(_.getIntField <= 300)

  override def beforeAll(): Unit = {
    val sc = ScioContext()
    val avroData = sc.parallelize(avroRecords)
    avroData.saveAsParquetAvroFile(testMultiDir.getAbsolutePath, conf = multiRowGroupConf)
    avroData.saveAsParquetAvroFile(testSingleDir.getAbsolutePath, conf = singleRowGroupConf)
    sc.run()
  }
  override def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  private def listFiles(dir: File): Seq[String] =
    Files
      .list(dir.toPath)
      .iterator()
      .asScala
      .map(_.toFile.toPath.toString)
      .toSeq

  "readAvroGenericRecordFiles" should "work without a projection" in {
    val schema = TestRecord.getClassSchema
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)
    runWithRealContext(options) { sc =>
      val result = sc
        .parallelize(listFiles(testSingleDir))
        .readFiles(
          ParquetRead.readAvroGenericRecordFiles(
            schema,
            predicate = predicate
          )
        )
      // downcast TestRecord to GenericRecord
      val expected: Seq[GenericRecord] = filteredRecords
      result should containInAnyOrder(expected)
    }
  }

  it should "work with a projection" in {
    val schema = TestRecord.getClassSchema
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)
    forAll(rowGroups) { path =>
      runWithRealContext(options) { sc =>
        val result = sc
          .parallelize(listFiles(path))
          .readFiles(
            ParquetRead.readAvroGenericRecordFiles(
              schema,
              projection,
              predicate
            )
          )
        // downcast TestRecord to GenericRecord
        val expected: Seq[GenericRecord] = filteredProjectedRecords
        result should containInAnyOrder(expected)
      }
    }
  }

  it should "be serializable" in {
    val schema = TestRecord.getClassSchema
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvroGenericRecordFiles(
        schema,
        projection,
        predicate
      )
    )
  }

  "readAvroFiles" should "work without a projection" in {
    forAll(rowGroups) { path =>
      runWithRealContext(options) { sc =>
        val result = sc
          .parallelize(listFiles(path))
          .readFiles[TestRecord](
            ParquetRead.readAvroFiles[TestRecord](
              predicate = predicate
            )
          )
        result should containInAnyOrder(filteredRecords)
      }
    }
  }

  it should "work with a projection on files with multiple row groups" in {
    runWithRealContext(options) { sc =>
      val result = sc
        .parallelize(listFiles(testMultiDir))
        .readFiles(
          ParquetRead.readAvroFiles[TestRecord](
            projection,
            predicate
          )
        )
      result should containInAnyOrder(filteredProjectedRecords)
    }
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvroFiles[TestRecord](
        projection,
        predicate
      )
    )
  }
}
