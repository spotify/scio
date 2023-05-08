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
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.io.api.Binary
import org.scalatest.BeforeAndAfterAll

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

case class Record(strField: String)

class ParquetReadFnTest extends PipelineSpec with BeforeAndAfterAll {
  private val typedRecords = (1 to 250).map(i => Record(i.toString)).toList
  private val avroRecords = (251 to 500).map(i =>
    TestRecord
      .newBuilder()
      .setIntField(i)
      .setStringField(i.toString)
      .build
  )

  private val directory = {
    val d = Files.createTempDirectory("parquet")
    d.toFile.deleteOnExit()
    d.toString
  }

  override def beforeAll(): Unit = {
    // Multiple row-groups
    val multiRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 16)

    // Single row-group
    val singleRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 1073741824)

    val sc = ScioContext()
    val typedData = sc.parallelize(typedRecords)
    val genericData = sc.parallelize(avroRecords)

    typedData.saveAsTypedParquetFile(s"$directory/typed/multi", conf = multiRowGroupConf)
    typedData.saveAsTypedParquetFile(s"$directory/typed/single", conf = singleRowGroupConf)

    genericData.saveAsParquetAvroFile(s"$directory/avro/multi", conf = multiRowGroupConf)
    genericData.saveAsParquetAvroFile(s"$directory/avro/single", conf = singleRowGroupConf)

    sc.run()
  }

  "Parquet ReadFn" should "read at file-level granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/typed/multi"))
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
      .parallelize(listFiles(s"$directory/typed/multi"))
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
        s"$directory/typed/multi/*.parquet",
        conf = granularityConf
      ) should containInAnyOrder(typedRecords)
  }

  it should "read at row-group granularity for files with a single row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    sc
      .typedParquetFile[Record](
        s"$directory/typed/single/*.parquet",
        conf = granularityConf
      ) should containInAnyOrder(typedRecords)
  }

  "readTyped" should "work with a predicate" in {
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/typed/single"))
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
      .parallelize(listFiles(s"$directory/typed/single"))
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
    val projection = Projection[TestRecord](_.getIntField)
    val expectedOut: Seq[GenericRecord] = (251 to 300).map { i =>
      new GenericRecordBuilder(projection).set("int_field", i).build()
    }

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/single"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          predicate = Predicate[TestRecord](_.getIntField <= 300)
        )
      ) should containInAnyOrder(expectedOut)

    sc.run()
  }

  it should "work with a projection and projectionFn" in {
    val projection = Projection[TestRecord](_.getIntField)

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/single"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          _.get("int_field").toString.toInt,
          predicate = Predicate[TestRecord](_.getIntField <= 300),
          conf = null
        )
      ) should containInAnyOrder(251 to 300)

    sc.run()
  }

  it should "work with a projection and projectionFn on files with multiple row groups" in {
    val projection = Projection[TestRecord](_.getIntField)

    implicit val coder = Coder.avroGenericRecordCoder(projection)
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/multi"))
      .readFiles(
        ParquetRead.readAvroGenericRecordFiles(
          projection,
          _.get("int_field").toString.toInt,
          predicate = Predicate[TestRecord](_.getIntField <= 300),
          conf = null
        )
      ) should containInAnyOrder(251 to 300)

    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvroGenericRecordFiles(
        Projection[TestRecord](_.getIntField),
        _.get("int_field").toString.toInt,
        predicate = Predicate[TestRecord](_.getIntField <= 300),
        conf = null
      )
    )
  }

  "readAvroSpecificRecordFiles" should "work without a projection or a projectionFn" in {
    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/single"))
      .readFiles(
        ParquetRead.readAvroSpecificRecordFiles[TestRecord](
          predicate = Predicate[TestRecord](_.getIntField == 300)
        )
      ) should containSingleValue(
      TestRecord.newBuilder().setIntField(300).setStringField("300").build()
    )
    sc.run()
  }

  it should "work with a projection but not a projectionFn" in {
    val projection = Projection[TestRecord](_.getIntField)
    implicit val coder = Coder.beam(AvroCoder.of(classOf[TestRecord], projection))

    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/single"))
      .readFiles(
        ParquetRead.readAvroSpecificRecordFiles[TestRecord](
          projection,
          Predicate[TestRecord](_.getIntField == 300)
        )
      ) should containSingleValue(TestRecord.newBuilder().setIntField(300).build())
    sc.run()
  }

  it should "work with a projection and a projectionFn" in {
    val projection = Projection[TestRecord](_.getIntField)
    implicit val coder = Coder.beam(AvroCoder.of(classOf[TestRecord], projection))

    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/single"))
      .readFiles(
        ParquetRead.readAvroSpecificRecordFiles(
          projection,
          (tr: TestRecord) => tr.getIntField.toInt,
          Predicate[TestRecord](_.getIntField == 300),
          null
        )
      ) should containSingleValue(300)
    sc.run()
  }

  it should "work with a projection and a projectionFn on files with multiple row groups" in {
    val projection = Projection[TestRecord](_.getIntField)
    implicit val coder = Coder.beam(AvroCoder.of(classOf[TestRecord], projection))

    val sc = ScioContext()
    sc
      .parallelize(listFiles(s"$directory/avro/multi"))
      .readFiles(
        ParquetRead.readAvroSpecificRecordFiles(
          projection,
          (tr: TestRecord) => tr.getIntField.toInt,
          Predicate[TestRecord](_.getIntField == 300),
          null
        )
      ) should containSingleValue(300)
    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readAvroSpecificRecordFiles(
        Projection[TestRecord](_.getIntField),
        (tr: TestRecord) => tr.getIntField.toInt,
        Predicate[TestRecord](_.getIntField == 300),
        null
      )
    )
  }

  private def listFiles(dir: String): Seq[String] =
    Files
      .list(Paths.get(dir))
      .map(_.toFile.toPath.toString)
      .iterator()
      .asScala
      .toSeq
}
