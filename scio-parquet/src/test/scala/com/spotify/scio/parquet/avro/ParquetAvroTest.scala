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

import java.io.File

import com.spotify.scio._
import com.spotify.scio.avro.{AvroUtils, TestRecord}
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.scalatest._

object ParquetAvroJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parquetAvroFile[TestRecord](args("input"))
      .saveAsAvroFile(args("output"))
    sc.close()
  }
}

class ParquetAvroTest extends TapSpec with BeforeAndAfterAll {

  private val dir = tmpDir
  private val specificFile = new File(dir, "specific.parquet")
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

  override protected def beforeAll(): Unit = {
    val specificWriter = AvroParquetWriter.builder[TestRecord](new Path(specificFile.toString))
      .withSchema(TestRecord.getClassSchema)
      .build()
    specificRecords.foreach(specificWriter.write)
    specificWriter.close()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(dir)

  "ParquetAvro" should "read specific records" in {
    val sc = ScioContext()
    val data = sc.parquetAvroFile[TestRecord](specificFile.toString)
    data should containInAnyOrder (specificRecords)
    sc.close()
  }

  it should "read specific records with projection" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val data = sc.parquetAvroFile[TestRecord](specificFile.toString, projection = projection)
    data.map(_.getIntField.toInt) should containInAnyOrder (1 to 10)
    data should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null
    }
    sc.close()
  }

  it should "read specific records with predicate" in {
    val sc = ScioContext()
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data = sc.parquetAvroFile[TestRecord](specificFile.toString, predicate = predicate)
    data should containInAnyOrder (specificRecords.filter(_.getIntField <= 5))
    sc.close()
  }

  it should "read specific records with projection and predicate" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val predicate = Predicate[TestRecord](_.getIntField <= 5)
    val data = sc.parquetAvroFile[TestRecord](specificFile.toString, projection, predicate)
    data.map(_.getIntField.toInt) should containInAnyOrder (1 to 5)
    data should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null
    }
    sc.close()
  }

  it should "work with JobTest" in {
    JobTest[ParquetAvroJob.type]
      .args("--input=in.parquet", "--output=out.avro")
      .input(AvroIO("in.parquet"), specificRecords)
      .output(AvroIO[TestRecord]("out.avro"))(_ should containInAnyOrder (specificRecords))
      .run()
  }

}
