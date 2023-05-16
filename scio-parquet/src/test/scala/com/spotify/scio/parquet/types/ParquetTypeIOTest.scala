/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.parquet.types

import java.{lang => jl}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ClosedTap, FileNamePolicySpec, TapSpec}
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.scalatest.BeforeAndAfterAll

class ParquetTypeIOFileNamePolicyTest extends FileNamePolicySpec[Wide] {
  val extension: String = ".parquet"
  def save(
    filenamePolicySupplier: FilenamePolicySupplier = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[Wide] = {
    in.map(i => Wide(i, i.toString, Some(i), (1 to i).toList))
      .saveAsTypedParquetFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[Wide]] = Seq(
    _.map(i => Wide(i, i.toString, Some(i), (1 to i).toList)).saveAsTypedParquetFile(
      "nonsense",
      shardNameTemplate = "NNN-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ParquetTypeIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {
  private val dir = tmpDir
  private val records = (1 to 10).map(newRecord)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(records)
      .saveAsTypedParquetFile(dir.toString)
    sc.run()
    ()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(dir)

  private def newRecord(i: Int): Wide = Wide(i, i.toString, Some(i), (1 to i).toList)

  private val predicate = FilterApi.or(
    FilterApi.ltEq(FilterApi.intColumn("i"), jl.Integer.valueOf(5)),
    FilterApi.gtEq(FilterApi.intColumn("o"), jl.Integer.valueOf(95))
  )

  "ParquetTypeIO" should "work" in {
    val xs = (1 to 100).map(newRecord)
    testTap(xs)(_.saveAsTypedParquetFile(_))(".parquet")
    testJobTest(xs)(ParquetTypeIO(_))(_.typedParquetFile[Wide](_))(
      _.saveAsTypedParquetFile(_)
    )
  }

  it should "read case classes" in {
    val sc = ScioContext()
    val data = sc.typedParquetFile[Wide](s"$dir/*.parquet")
    data should containInAnyOrder(records)
    sc.run()
    ()
  }

  it should "read case classes with projection" in {
    val sc = ScioContext()
    val data = sc.typedParquetFile[Narrow](s"$dir/*.parquet")
    data should containInAnyOrder(records.map(r => Narrow(r.i, r.r)))
    sc.run()
    ()
  }

  it should "read case classes with predicate" in {
    val sc = ScioContext()
    val data = sc.typedParquetFile[Wide](s"$dir/*.parquet", predicate = predicate)
    data should containInAnyOrder(records.filter(t => t.i <= 5 || t.o.exists(_ >= 95)))
    sc.run()
    ()
  }

  it should "read case classes with projection and predicate" in {
    val sc = ScioContext()
    val data = sc.typedParquetFile[Narrow](s"$dir/*.parquet", predicate = predicate)
    val expected = records.filter(t => t.i <= 5 || t.o.exists(_ >= 95)).map(t => Narrow(t.i, t.r))
    data should containInAnyOrder(expected)
    sc.run()
    ()
  }
}

case class Wide(i: Int, s: String, o: Option[Int], r: List[Int])
case class Narrow(i: Int, r: List[Int])
