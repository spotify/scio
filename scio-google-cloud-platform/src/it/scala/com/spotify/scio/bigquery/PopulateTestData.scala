/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery

import java.nio.channels.Channels
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import com.google.api.services.bigquery.model.{Dataset, DatasetReference}
import com.google.protobuf.ByteString
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.client.BigQuery
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.MimeTypes
import org.joda.time._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object PopulateTestData {
  def main(args: Array[String]): Unit = {
    BigQueryTestData.populate("data-integration-test")
    GcsTestData.populate("neville-test-us")
  }
}

object GcsTestData {
  def populate(bucket: String): Unit = {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create())

    val root = Paths.get("src/it/resources")
    Files
      .walk(root)
      .collect(Collectors.toList[Path])
      .asScala
      .filter(Files.isRegularFile(_))
      .foreach { src =>
        val resourceId =
          FileSystems.matchNewResource(s"gs://$bucket/${root.relativize(src)}", false)
        val dst = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.BINARY))
        Files.copy(src, dst)
        dst.close()
      }
  }
}

object BigQueryTestData {
  @BigQueryType.toTable
  case class Shakespeare(
    corpus_date: Option[Int],
    corpus: Option[String],
    word_count: Option[Int],
    word: Option[String]
  )

  @BigQueryType.toTable
  case class ToTableT(word: String, word_count: Int)

  @BigQueryType.toTable
  case class Required(
    bool: Boolean,
    int: Long,
    float: Double,
    numeric: BigDecimal,
    string: String,
    bytes: ByteString,
    timestamp: Instant,
    date: LocalDate,
    time: LocalTime,
    datetime: LocalDateTime
  )

  @BigQueryType.toTable
  case class Optional(
    bool: Option[Boolean],
    int: Option[Long],
    float: Option[Double],
    numeric: Option[BigDecimal],
    string: Option[String],
    bytes: Option[ByteString],
    timestamp: Option[Instant],
    date: Option[LocalDate],
    time: Option[LocalTime],
    datetime: Option[LocalDateTime]
  )

  @BigQueryType.toTable
  case class Repeated(
    bool: List[Boolean],
    int: List[Long],
    float: List[Double],
    numeric: List[BigDecimal],
    string: List[String],
    bytes: List[ByteString],
    timestamp: List[Instant],
    date: List[LocalDate],
    time: List[LocalTime],
    datetime: List[LocalDateTime]
  )

  case class Record(int: Long, string: String)

  @BigQueryType.toTable
  case class Nested(required: Record, optional: Option[Record], repeated: List[Record])

  def populate(projectId: String): Unit = {
    val bq = BigQuery.defaultInstance()

    // TypedBigQueryIT
    ensureDatasetExists(bq, projectId, "bigquery_avro_it", "EU")

    // DynamicBigQueryIT
    ensureDatasetExists(bq, projectId, "bigquery_dynamic_it", "EU")

    // BeamSchemaIT
    ensureDatasetExists(bq, projectId, "schema_it", "EU")

    // BigQueryPartitionUtilIT
    ensureDatasetExists(bq, projectId, "samples_eu", "EU")
    ensureDatasetExists(bq, projectId, "samples_us", "US")
    ensureDatasetExists(bq, projectId, "partition_a", "EU")
    ensureDatasetExists(bq, projectId, "partition_b", "EU")
    ensureDatasetExists(bq, projectId, "partition_c", "EU")
    populatePartitionedTables(bq, projectId)

    // StorageIT
    ensureDatasetExists(bq, projectId, "storage", "EU")
    populateStorageTables(bq, projectId)
  }

  private def ensureDatasetExists(
    bq: BigQuery,
    projectId: String,
    datasetId: String,
    location: String
  ): Unit = {
    val ds = new Dataset()
      .setDatasetReference(new DatasetReference().setProjectId(projectId).setDatasetId(datasetId))
      .setLocation(location)
    try {
      bq.client.underlying.datasets().insert(projectId, ds).execute()
    } catch {
      case NonFatal(e) => e.printStackTrace()
    }
    ()
  }

  private def populatePartitionedTables(bq: BigQuery, projectId: String): Unit = {
    bq.writeTypedRows(
      s"$projectId:samples_eu.shakespeare",
      List(Shakespeare(Some(0), Some("sonnets"), Some(1), Some("LVII"))),
      WRITE_TRUNCATE
    )

    bq.writeTypedRows(
      s"$projectId:samples_us.shakespeare",
      List(Shakespeare(Some(0), Some("sonnets"), Some(1), Some("LVII"))),
      WRITE_TRUNCATE
    )

    val data = List(ToTableT("a", 1), ToTableT("b", 2))
    bq.writeTypedRows(s"$projectId:partition_a.table_20170101", data, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:partition_a.table_20170102", data, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:partition_a.table_20170103", data, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:partition_b.table_20170101", data, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:partition_b.table_20170102", data, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:partition_c.table_20170104", data, WRITE_TRUNCATE)
    ()
  }

  private def populateStorageTables(bq: BigQuery, projectId: String): Unit = {
    val required = (0 until 10).toList.map(newRequired)
    val optional = (0 until 10).toList.map(newOptional)
    val repeated = (0 until 10).toList.map(newRepeated)
    val nested = (0 until 10).toList.map { i =>
      val r = Record(i, s"s$i")
      Nested(r, Some(r), List(r))
    }

    bq.writeTypedRows(s"$projectId:storage.required", required, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:storage.optional", optional, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:storage.repeated", repeated, WRITE_TRUNCATE)
    bq.writeTypedRows(s"$projectId:storage.nested", nested, WRITE_TRUNCATE)
    ()
  }

  private def newRequired(i: Int): Required = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Required(
      true,
      i,
      i,
      BigDecimal(i),
      s"s$i",
      ByteString.copyFromUtf8(s"s$i"),
      t.plus(Duration.millis(i)),
      dt.toLocalDate.plusDays(i),
      dt.toLocalTime.plusMillis(i),
      dt.toLocalDateTime.plusMillis(i)
    )
  }

  private def newOptional(i: Int): Optional = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Optional(
      Some(true),
      Some(i),
      Some(i),
      Some(BigDecimal(i)),
      Some(s"s$i"),
      Some(ByteString.copyFromUtf8(s"s$i")),
      Some(t.plus(Duration.millis(i))),
      Some(dt.toLocalDate.plusDays(i)),
      Some(dt.toLocalTime.plusMillis(i)),
      Some(dt.toLocalDateTime.plusMillis(i))
    )
  }

  private def newRepeated(i: Int): Repeated = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Repeated(
      List(true),
      List(i),
      List(i),
      List(BigDecimal(i)),
      List(s"s$i"),
      List(ByteString.copyFromUtf8(s"s$i")),
      List(t.plus(Duration.millis(i))),
      List(dt.toLocalDate.plusDays(i)),
      List(dt.toLocalTime.plusMillis(i)),
      List(dt.toLocalDateTime.plusMillis(i))
    )
  }
}
