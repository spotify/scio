/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.parquet

import com.spotify.scio.avro._
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import magnolify.parquet.{ArrayEncoding, MagnolifyParquetProperties, ParquetType}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.parquet.avro.AvroWriteSupport

import scala.jdk.CollectionConverters._
import java.io.File

private case class Nested(i: Int)
private case class TestRecordScala(a: Int, b: List[String], c: List[Nested], d: Map[String, Int])

object ParquetFormatInteropTest {
  val AvroSchema = new Schema.Parser().parse(s"""|{
       |  "type":"record",
       |  "name":"TestRecord",
       |  "namespace":"com.spotify.scio.parquet",
       |  "fields":[
       |    {"name":"a","type":"int"},
       |    {"name":"b","type":{"type":"array","items":"string"}},
       |    {"name":"c","type":{"type":"array","items":{
       |      "type":"record","name":"array","namespace":"","fields":[{"name":"i","type":"int"}]
       |    }}},
       |    {"name":"d","type":{"type":"map","values":"int"}}]}
       |    """.stripMargin)
}
class ParquetFormatInteropTest extends PipelineSpec with TapSpec {
  import ParquetFormatInteropTest.AvroSchema

  private val nestedSchema = AvroSchema.getField("c").schema().getElementType

  private val genericRecords: Seq[GenericRecord] = (1 to 10).map { i =>
    val nested = new GenericData.Record(nestedSchema)
    nested.put("i", i)
    val record = new GenericData.Record(AvroSchema)
    record.put("a", i)
    record.put("b", List(i, i * 2).map(_.toString).asJava)
    record.put("c", List(nested).asJava)
    record.put("d", Map("x" -> Integer.valueOf(i)).asJava)
    record
  }

  private val typedRecords = (1 to 10).map { i =>
    TestRecordScala(i, List(i, i * 2).map(_.toString), List(Nested(i)), Map("x" -> i))
  }

  implicit val grCoder: com.spotify.scio.coders.Coder[GenericRecord] =
    avroGenericRecordCoder(AvroSchema)

  private val ptUngroupedListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.Ungrouped
    }
  )

  private val ptOldListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.ThreeLevelArray
    }
  )

  private val ptNewListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.ThreeLevelList
    }
  )

  ".typedParquetFile" should "be able to read data written with .saveAsParquetAvroFile with old list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptOldListEncoding

      runWithRealContext()(
        _.parallelize(genericRecords)
          .saveAsParquetAvroFile(dir.toString, schema = AvroSchema)
      )

      runWithRealContext()(
        _.typedParquetFile[TestRecordScala](s"$dir/*.parquet")
          .map(identity) should containInAnyOrder(typedRecords)
      )
  }

  it should "be able to read data written with .saveAsParquetAvroFile with new list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      val listConf = ParquetConfiguration.of(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE -> false)

      runWithRealContext()(
        _.parallelize(genericRecords)
          .saveAsParquetAvroFile(dir.toString, schema = AvroSchema, conf = listConf)
      )

      runWithRealContext()(
        _.typedParquetFile[TestRecordScala](s"$dir/*.parquet")
          .map(identity) should containInAnyOrder(typedRecords)
      )
  }

  def testFailOnMismatchedReadWriteEncodings(
    writeEncoding: ParquetType[TestRecordScala],
    readEncoding: ParquetType[TestRecordScala],
    dir: File
  ): Unit = {
    {
      implicit val ptWrite: ParquetType[TestRecordScala] = writeEncoding
      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )
    }
    {
      val e = the[PipelineExecutionException] thrownBy {
        implicit val ptRead: ParquetType[TestRecordScala] = readEncoding
        runWithRealContext()(
          _.typedParquetFile[TestRecordScala](s"$dir/*.parquet")
            .map(identity) should containInAnyOrder(typedRecords)
        )
      }
      e.getCause.getClass shouldBe classOf[org.apache.parquet.io.InvalidRecordException]
    }
  }

  it should "fail quickly when reading data written with new list encoding if read ArrayEncoding is set to 2 level" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptNewListEncoding, ptOldListEncoding, dir)
  }

  it should "fail quickly when reading data written with new list encoding if read ArrayEncoding is set to ungrouped" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptNewListEncoding, ptUngroupedListEncoding, dir)
  }

  it should "fail quickly when reading data written with old list encoding if read ArrayEncoding is set to new encoding" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptOldListEncoding, ptNewListEncoding, dir)
  }

  it should "fail quickly when reading data written with old list encoding if read ArrayEncoding is set to ungrouped" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptOldListEncoding, ptUngroupedListEncoding, dir)
  }

  ".parquetAvroFile" should "be able to read data written with .saveAsTypedParquetFile with old list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptOldListEncoding

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      runWithRealContext()(
        _.parquetAvroFile[GenericRecord](s"$dir/*.parquet", projection = pt.avroSchema)
          .map(identity) should containInAnyOrder(genericRecords)
      )
  }

  it should "be able to read data written with .saveAsTypedParquetFile with new list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      val listConf = ParquetConfiguration.of(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE -> false)

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      runWithRealContext()(
        _.parquetAvroFile[GenericRecord](
          s"$dir/*.parquet",
          projection = pt.avroSchema,
          conf = listConf
        )
          .map(identity) should containInAnyOrder(genericRecords)
      )
  }
}
