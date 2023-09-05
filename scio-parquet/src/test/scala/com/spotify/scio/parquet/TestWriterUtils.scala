/*
 * Copyright 2023 Spotify AB.
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

import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.{Account, AccountStatus}
import org.apache.commons.io.FileUtils
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class TestWriterUtils extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private lazy val testDir = Files.createTempDirectory("scio-parquet-writer-utils-test-").toFile

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  "WriterUtils" should "set configuration values correctly" in {
    val path = testDir.toPath.resolve("avro-files")

    val sc = ScioContext()
    sc.parallelize(1 to 10)
      .map { r =>
        Account
          .newBuilder()
          .setId(r)
          .setType("checking")
          .setName(r.toString)
          .setAmount(r.doubleValue)
          .setAccountStatus(AccountStatus.Active)
          .build()
      }
      .saveAsParquetAvroFile( // WriterUtils has too many protected classes to invoke directly; test via Scio write
        path.toFile.getAbsolutePath,
        numShards = 1,
        conf = ParquetConfiguration.of(
          "parquet.enable.dictionary" -> false,
          "parquet.enable.dictionary#account_status" -> true,
          "parquet.bloom.filter.enabled" -> true,
          "parquet.bloom.filter.enabled#id" -> false
        )
      )

    sc.run()

    val columnEncodings = getColumnEncodings(path.resolve("part-00000-of-00001.parquet"))

    assertColumn(
      columnEncodings(0),
      "id",
      hasBloomFilter = false,
      Seq(Encoding.BIT_PACKED, Encoding.PLAIN)
    )
    assertColumn(
      columnEncodings(1),
      "type",
      hasBloomFilter = true,
      Seq(Encoding.BIT_PACKED, Encoding.PLAIN)
    )
    assertColumn(
      columnEncodings(2),
      "name",
      hasBloomFilter = true,
      Seq(
        Encoding.BIT_PACKED,
        Encoding.RLE, // RLE encoding is used for optional fields
        Encoding.PLAIN
      )
    )
    assertColumn(
      columnEncodings(3),
      "amount",
      hasBloomFilter = true,
      Seq(Encoding.BIT_PACKED, Encoding.PLAIN)
    )
    assertColumn(
      columnEncodings(4),
      "account_status",
      hasBloomFilter = true,
      Seq(Encoding.BIT_PACKED, Encoding.RLE, Encoding.PLAIN_DICTIONARY)
    )
  }

  private def getColumnEncodings(path: Path): List[ColumnChunkMetaData] = {
    val options = HadoopReadOptions.builder(ParquetConfiguration.empty()).build
    val r = ParquetFileReader.open(BeamInputFile.of(path.toFile.getAbsolutePath), options)
    assert(r.getRowGroups.size() == 1)

    val columns = r.getRowGroups.get(0).getColumns.asScala.toList
    r.close()
    columns
  }

  private def assertColumn(
    column: ColumnChunkMetaData,
    name: String,
    hasBloomFilter: Boolean,
    encodings: Iterable[Encoding]
  ): Unit = {
    column.getPath.asScala should contain only name
    column.getEncodings.asScala should contain theSameElementsAs encodings
    if (hasBloomFilter) {
      column.getBloomFilterOffset should be > -1L
    } else {
      column.getBloomFilterOffset shouldBe -1L
    }
  }
}
