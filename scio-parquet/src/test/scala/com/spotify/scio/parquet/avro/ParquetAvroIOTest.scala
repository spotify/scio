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
import com.spotify.scio.avro._
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils
import org.scalatest._

class ParquetAvroIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {

  private val dir = tmpDir
  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(specificRecords)
      .saveAsParquetAvroFile(dir.toString)
    sc.close()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(dir)

  "ParquetAvroIO" should "work with specific records" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(ParquetAvroIO(_))(
      _.parquetAvroFile(_).map(identity))(_.saveAsParquetAvroFile(_))(".parquet")
    testJobTest(xs)(
      ParquetAvroIO(_))(_.parquetAvroFile[TestRecord](_).map(identity))(_.saveAsParquetAvroFile(_))
  }

  it should "read specific records with projection" in {
    val sc = ScioContext()
    val projection = Projection[TestRecord](_.getIntField)
    val data = sc.parquetAvroFile[TestRecord](dir + "/*.parquet", projection = projection)
    data.map(_.getIntField.toInt) should containInAnyOrder (1 to 10)
    data.map(identity) should forAll[TestRecord] { r =>
      r.getLongField == null && r.getFloatField == null && r.getDoubleField == null &&
        r.getBooleanField == null && r.getStringField == null && r.getArrayField.size() == 0
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
        r.getBooleanField == null && r.getStringField == null && r.getArrayField.size() == 0
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
    val expected = nestedRecords.map(_.getName.toString)
    data.map(_.getName.toString) should containInAnyOrder (expected)
    data.flatMap(a => Some(a.getName.toString)) should containInAnyOrder (expected)
    sc2.close()

    FileUtils.deleteDirectory(dir)
  }

  it should "write generic records" in {
    val dir = tmpDir

    val genericRecords = (1 to 100).map(AvroUtils.newGenericRecord)
    val sc = ScioContext()
    sc.parallelize(genericRecords)
      .saveAsParquetAvroFile(dir.toString, numShards = 1, schema = AvroUtils.schema)
    sc.close()

    val files = dir.listFiles()
    files.length shouldBe 1

    val params =
      ParquetAvroIO.ReadParam[GenericRecord, GenericRecord](AvroUtils.schema, null, identity)
    val tap = ParquetAvroTap(files.head.getAbsolutePath, params)
    tap.value.toList should contain theSameElementsAs genericRecords

    FileUtils.deleteDirectory(dir)
  }

}
