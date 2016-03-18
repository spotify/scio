/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.scio.hdfs

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.bigquery._
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import scala.concurrent.Future

class HdfsTapTest extends PipelineSpec {

  "Future" should "support saveAsHdfsAvroFile with SpecificRecord" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsHdfsAvroFile(dir.getPath)
    }
    verifyTap(t, Set(1, 2, 3).map(newSpecificRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsHdfsAvroFile with GenericRecord" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsHdfsAvroFile(dir.getPath, schema = newGenericRecord(1).getSchema)
    }
    verifyTap(t, Set(1, 2, 3).map(newGenericRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsHdfsAvroFile with reflect record" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c"))
        .map(s => ByteBuffer.wrap(s.getBytes))
        .saveAsHdfsAvroFile(dir.getPath, schema = new Schema.Parser().parse("\"bytes\""))
    }.map(b => new String(b.array()))
    verifyTap(t, Set("a", "b", "c"))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsHdfsTextFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(i => newTableRow(i).toString)
        .saveAsHdfsTextFile(dir.getPath)
    }
    verifyTap(t, Set(1, 2, 3).map(i => newTableRow(i).toString))
    FileUtils.deleteDirectory(dir)
  }

  // TODO: how to reuse test code from scio-test?

  def runWithFileFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] = {
    val sc = ScioContext()
    val f = fn(sc)
    sc.close()
    f.waitForResult()
  }

  def verifyTap[T](tap: Tap[T], expected: Set[T]): Unit = {
    tap.value.toSet should equal (expected)
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder (expected)
    sc.close()
  }

  def tmpDir: File = new File(new File(sys.props("java.io.tmpdir")), "scio-test-" + UUID.randomUUID().toString)

  def newGenericRecord(i: Int): GenericRecord = {
    def f(name: String, tpe: Schema.Type) =
      new Schema.Field(
        name,
        Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
        null, null)

    val schema = Schema.createRecord("GenericTestRecord", null, null, false)
    schema.setFields(List(
      f("int_field", Schema.Type.INT),
      f("long_field", Schema.Type.LONG),
      f("float_field", Schema.Type.FLOAT),
      f("double_field", Schema.Type.DOUBLE),
      f("boolean_field", Schema.Type.BOOLEAN),
      f("string_field", Schema.Type.STRING)
    ).asJava)

    val r = new GenericData.Record(schema)
    r.put("int_field", 1 * i)
    r.put("long_field", 1L * i)
    r.put("float_field", 1F * i)
    r.put("double_field", 1.0 * i)
    r.put("boolean_field", true)
    r.put("string_field", "hello")
    r
  }

  def newSpecificRecord(i: Int): TestRecord = new TestRecord(i, i.toLong, i.toFloat, i.toDouble, true, "hello")

  def newTableRow(i: Int): TableRow = TableRow(
    "int_field" -> 1 * i,
    "long_field" -> 1L * i,
    "float_field" -> 1F * i,
    "double_field" -> 1.0 * i,
    "boolean_field" -> "true",
    "string_field" -> "hello")

}
