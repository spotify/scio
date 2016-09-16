/*
 * Copyright 2016 Spotify AB.
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
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.test.TestRecord
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import scala.concurrent.Future
import scala.reflect.ClassTag

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

  it should "support default compression with saveAsHdfsAvroFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsHdfsAvroFile(dir.getPath)
    }
    val avroFile = new File(dir, "part-r-00000.avro")
    val dataFileReader = new DataFileReader(avroFile, new SpecificDatumReader[TestRecord])
    new String(dataFileReader.getMeta("avro.codec")) shouldBe "deflate"
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

  it should "support user conf with saveAsHdfsAvroFile" in {
    val dir = tmpDir
    // create empty configuration (no compresion)
    val conf = new Configuration(false)
    val t = runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsHdfsAvroFile(dir.getPath, conf=conf)
    }
    val avroFile = new File(dir, "part-r-00000.avro")
    val dataFileReader = new DataFileReader(avroFile, new SpecificDatumReader[TestRecord])
    new String(dataFileReader.getMeta("avro.codec")) shouldBe "null"
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsHdfsTextFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c"))
        .saveAsHdfsTextFile(dir.getPath)
    }
    verifyTap(t, Set("a", "b", "c"))
    FileUtils.deleteDirectory(dir)
  }

  it should "support default compression with saveAsHdfsTextFile" in {
    val dir = tmpDir
    val t = runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c"))
        .saveAsHdfsTextFile(dir.getPath)
    }
    new File(dir, "part-r-00000") should not (exist)
    new File(dir, "part-r-00000.deflate") should exist
    FileUtils.deleteDirectory(dir)
  }

  it should "support user conf with saveAsHdfsTextFile" in {
    val dir = tmpDir
    // create empty configuration (no compresion)
    val conf = new Configuration(false)
    val t =  runWithFileFuture {
      _
        .parallelize(Seq("a", "b", "c", "d"))
        .saveAsHdfsTextFile(dir.getPath, conf = conf)
    }
    new File(dir, "part-r-00000") should exist
    new File(dir, "part-r-00000.deflate") should not (exist)
    FileUtils.deleteDirectory(dir)
  }

  // TODO: how to reuse test code from scio-test?

  def runWithFileFuture[T](fn: ScioContext => Future[Tap[T]]): Tap[T] = {
    val sc = ScioContext()
    val f = fn(sc)
    sc.close()
    f.waitForResult()
  }

  def verifyTap[T: ClassTag](tap: Tap[T], expected: Set[T]): Unit = {
    tap.value.toSet should equal (expected)
    val sc = ScioContext()
    tap.open(sc) should containInAnyOrder (expected)
    sc.close()
  }

  def tmpDir: File = new File(
    new File(sys.props("java.io.tmpdir")),
    "scio-test-" + UUID.randomUUID().toString)

}
