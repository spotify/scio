/*
 * Copyright 2017 Spotify AB.
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
import com.spotify.scio.avro.{Account, AvroUtils, TestRecord}
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.scalatest._

object ParquetAvroJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parquetAvroFile[TestRecord](args("input"))
      .saveAsParquetAvroFile(args("output"))
    sc.close()
  }
}

object Test {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parallelize((1 to 100).map(AvroUtils.newSpecificRecord))
      .saveAsParquetAvroFile(args("output"), numShards = 1)
    sc.close()
  }
}

class ParquetAvroTest extends TapSpec with BeforeAndAfterAll {

  private val dir = tmpDir
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(specificRecords)
      .saveAsParquetAvroFile(dir.toString)
    sc.close()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(dir)

  "ParquetAvro" should "read specific records" in {
    val sc = ScioContext()
    val data = sc.parquetAvroFile[TestRecord](dir + "/*.parquet")
    data.map(identity) should containInAnyOrder (specificRecords)
    sc.close()
  }

  it should "read specific records with projection" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val data = sc.parquetAvroFile[TestRecord](dir + "/*.parquet", projection = projection)
    data.map(_.getIntField.toInt) should containInAnyOrder (1 to 10)
    data.map(identity) should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null
    }
    sc.close()
  }

  it should "read specific records with predicate" in {
    val sc = ScioContext()
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data = sc.parquetAvroFile[TestRecord](dir + "/*.parquet", predicate = predicate)
    data.map(identity) should containInAnyOrder (specificRecords.filter(_.getIntField <= 5))
    sc.close()
  }

  it should "read specific records with projection and predicate" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data = sc.parquetAvroFile[TestRecord](dir + "/*.parquet", projection, predicate)
    data.map(_.getIntField.toInt) should containInAnyOrder (1 to 5)
    data.map(identity) should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null
    }
    sc.close()
  }

  it should "read with incomplete projection" in {
    val dir = tmpDir

    val sc1 = ScioContext()
    val nestedRecords = (1 to 10).map(x => new Account(x, x.toString, x.toString, x.toDouble))
    sc1.parallelize(nestedRecords)
      .saveAsParquetAvroFile(dir.toString)
    sc1.close()

    val sc2 = ScioContext()
    val projection = Projection[Account](_.getName)
    val data = sc2.parquetAvroFile[Account](dir + "/*.parquet", projection = projection)
    data.map(_.getName.toString) should containInAnyOrder (nestedRecords.map(_.getName.toString))
    sc2.close()

    FileUtils.deleteDirectory(dir)
  }

  it should "write generic records" in {
    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    val sc = ScioContext()
    val tmp = tmpDir
    sc.parallelize(genericRecords)
      .saveAsParquetAvroFile(tmp.toString, numShards = 1, schema = AvroUtils.schema)
    sc.close()

    val files = tmp.listFiles()
    files.length shouldBe 1

    val reader = AvroParquetReader.builder[GenericRecord](new Path(files(0).toString)).build()
    var r: GenericRecord = reader.read()
    val b = Seq.newBuilder[GenericRecord]
    while (r != null) {
      b += r
      r = reader.read()
    }
    b.result() should contain theSameElementsAs genericRecords

    reader.close()
    FileUtils.deleteDirectory(tmp)
  }

  it should "work with JobTest" in {
    JobTest[ParquetAvroJob.type]
      .args("--input=in.parquet", "--output=out.parquet")
      .input(AvroIO("in.parquet"), specificRecords)
      .output(AvroIO[TestRecord]("out.parquet"))(_ should containInAnyOrder (specificRecords))
      .run()
  }

}
