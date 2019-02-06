/*
 * Copyright 2019 Spotify AB.
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
package com.spotify

import com.spotify.scio._
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.bigquery.types.BigQueryType

/**
 * IO benchmark jobs, run once daily and logged to DataStore.
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioIOBenchmark.scala so
 * that it can run with past Scio releases.
 */
object ScioIOBenchmark {

  import ScioBatchBenchmark._

  private val benchmarks =
    ScioBenchmarkSettings.benchmarks("com\\.spotify\\.ScioIOBenchmark\\$[\\w]+\\$")

  // Has 823,280 records
  private val avroGcsPath =
    "gs://data-integration-test-benchmark-eu/avro-benchmark/part-*"

  // Has 55,250 records
  private val textGcsPath =
    "gs://data-integration-test-benchmark-eu/text-benchmark/part-*"

  @AvroType.toSchema
  case class Shakespeare(word: String, word_count: Long, corpus: String, corpus_date: Long)

  @BigQueryType.fromQuery("SELECT key, word FROM [bigquery_benchmarks.bigquery_read]")
  class Row

  @BigQueryType.toTable
  case class Words(key: Int, word: String)

  def main(args: Array[String]): Unit =
    BenchmarkRunner.runParallel(args, "ScioIOBenchmark", benchmarks)

  // Reads 823,280 records
  object AvroIORead extends Benchmark {
    import com.spotify.scio.avro._

    override def run(sc: ScioContext): Unit =
      sc.typedAvroFile[Shakespeare](avroGcsPath)
        .map(s => s.word + ": " + s.word_count)
  }

  // Reads 55,250 records
  object TextIORead extends Benchmark {
    override def run(sc: ScioContext): Unit =
      sc.textFile(textGcsPath)
        .map(_.split("\t"))
  }

  // Writes 10,000,000 records
  object BigQueryWrite extends Benchmark {
    import com.spotify.scio.bigquery._

    import scala.collection.JavaConverters._

    override def run(sc: ScioContext): Unit = {
      val table = "bigquery_benchmarks.bigquery_write"
      randomUUIDs(sc, 100 * 100000)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * 1000))
        .map(e => Words(e._1, e._2.elem))
        .saveAsTypedBigQuery(table, WRITE_TRUNCATE, CREATE_IF_NEEDED)
    }
  }

  // Reads 10,000,000 records
  object BigQueryRead extends Benchmark {
    import com.spotify.scio.bigquery._

    override def run(sc: ScioContext): Unit =
      sc.typedBigQuery[Row]()
        .map(r => (r.key, r.word))
  }
}
