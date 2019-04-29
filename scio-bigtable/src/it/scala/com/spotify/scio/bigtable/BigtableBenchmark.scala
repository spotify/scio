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

package com.spotify.scio.bigtable

import java.util.{List => JList}

import com.google.bigtable.v2.{ReadRowsRequest, RowFilter, RowSet}
import com.google.cloud.bigtable.config.{BigtableOptions => GBigtableOptions}
import com.google.cloud.bigtable.grpc.scanner.FlatRow
import com.google.cloud.bigtable.grpc.{BigtableInstanceName, BigtableSession}
import com.google.common.cache.CacheBuilder
import com.google.common.util.concurrent.{Futures, ListenableFuture, MoreExecutors}
import com.google.protobuf.ByteString
import com.spotify.scio.benchmarks._
import com.spotify.scio._
import com.spotify.scio.transforms.AsyncLookupDoFn
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV

import scala.util.{Failure, Success}

object BigtableBenchmark {
  val ProjectId: String = "data-integration-test"
  val InstanceId: String = "scio-bigtable-benchmark"
  val TableId: String = "scio-bigtable-benchmark"
  val PostfixWithOne: String = "0000000001"
  val PostfixWithZeroes: String = "0000000000"
  val MillionElements: Int = 1000000
  val MaxPendingRequests: Int = 10000

  val BigtableOptions: GBigtableOptions = new GBigtableOptions.Builder()
    .setProjectId(ProjectId)
    .setInstanceId(InstanceId)
    .setUserAgent("bigtable-test")
    .build()

  val FamilyName: String = "side"
  val ColumnQualifier: ByteString = ByteString.copyFromUtf8("value")

  val ReadRowsRequestTemplate: ReadRowsRequest = ReadRowsRequest
    .newBuilder()
    .setTableName(new BigtableInstanceName(ProjectId, InstanceId).toTableNameStr(TableId))
    .setFilter(
      RowFilter
        .newBuilder()
        .setFamilyNameRegexFilter(FamilyName)
        .setColumnQualifierRegexFilter(ColumnQualifier)
    )
    .setRowsLimit(1L)
    .build()

  val LowerLetters: Seq[String] = (0 until 26).map('a'.toInt + _).map(_.toChar.toString)
  val UpperLetters: Seq[String] = LowerLetters.map(_.toUpperCase)
  val Letters: Seq[String] = LowerLetters ++ UpperLetters

  class FillDoFn(val n: Int) extends DoFn[String, String] {
    @ProcessElement
    def processElement(c: DoFn[String, String]#ProcessContext): Unit = {
      val prefix = c.element()
      var i = 0
      while (i < n) {
        c.output("%s%010d".format(prefix, i))
        i += 1
      }
    }
  }

  def bigtableLookup(session: BigtableSession, input: String): ListenableFuture[String] = {
    // Perform lookup and recover from non-fatal exception with a fallback
    val future = readFlatRowsAsync(session, input)
    Futures.catching(
      future,
      classOf[NonFatalException],
      new com.google.common.base.Function[NonFatalException, String] {
        override def apply(input: NonFatalException): String = {
          assert(input.getMessage.endsWith(PostfixWithOne))
          "fallback"
        }
      },
      MoreExecutors.directExecutor()
    )
  }

  class NonFatalException(msg: String) extends RuntimeException(msg)

  class FatalException(msg: String) extends RuntimeException(msg)

  def readFlatRowsAsync(session: BigtableSession, input: String): ListenableFuture[String] =
    if (input.endsWith(PostfixWithOne)) {
      // Simulate a non-fatal exception, to be recovered with a fallback
      Futures.immediateFailedFuture(new NonFatalException(input))
    } else {
      val key = ByteString.copyFromUtf8(s"key-$input")
      val expected = ByteString.copyFromUtf8(s"val-$input")
      val request = ReadRowsRequestTemplate.toBuilder
        .setRows(RowSet.newBuilder().addRowKeys(key).build())
        .build()
      Futures.transform(
        session.getDataClient.readFlatRowsAsync(request),
        new com.google.common.base.Function[JList[FlatRow], String] {
          override def apply(input: JList[FlatRow]): String = {
            val result = input
            assert(result.size() == 1)
            val cells = result.get(0).getCells
            assert(result.get(0).getCells.size() == 1)
            val value = cells.get(0).getValue
            assert(value == expected)
            value.toStringUtf8
          }
        },
        MoreExecutors.directExecutor()
      )
    }

  def checkResult(kv: KV[String, AsyncLookupDoFn.Try[String]]): (Int, Int) =
    kv.getValue.asScala match {
      case Success(value) =>
        val expected = if (kv.getKey.endsWith(PostfixWithOne)) "fallback" else s"val-${kv.getKey}"
        assert(value == expected)
        (1, 0)
      case Failure(exception) =>
        assert(exception.isInstanceOf[FatalException])
        assert(kv.getKey.endsWith(PostfixWithZeroes))
        assert(kv.getKey == exception.getMessage)
        (0, 1)
    }

  def main(args: Array[String]): Unit = {
    // Run sequentially to avoid read/write contention
    BenchmarkRunner.runSequentially(
      args,
      "BigtableBenchmark",
      benchmarks,
      ScioBenchmarkSettings.commonArgs() :+ "--region=us-east1"
    )
  }

  private val benchmarks =
    ScioBenchmarkSettings
      .benchmarks("com\\.spotify\\.scio\\.bigtable\\.BigtableBenchmark\\$[\\w]+\\$")

  // Generate 52 million key value pairs
  object BigtableWrite extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      sc.ensureTables(BigtableOptions, Map(TableId -> List(FamilyName)))
      sc.parallelize(Letters)
        .applyTransform(ParDo.of(new FillDoFn(MillionElements)))
        .map { s =>
          val key = ByteString.copyFromUtf8(s"key-$s")
          val value = ByteString.copyFromUtf8(s"val-$s")
          val m = Mutations.newSetCell(FamilyName, ColumnQualifier, value, 0L)
          (key, Iterable(m))
        }
        .saveAsBigtable(BigtableOptions, TableId)
    }
  }

  // Read 52 million records from BigTable
  object BigtableRead extends Benchmark {
    override def run(sc: ScioContext): Unit =
      sc.bigtable(BigtableOptions, TableId, ByteKeyRange.ALL_KEYS, RowFilter.getDefaultInstance)
        .count
  }

  // Async key value lookup 52 million reads
  object AsyncBigtableDoFnRead extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      sc.parallelize(Letters)
        .applyTransform(ParDo.of(new FillDoFn(MillionElements)))
        .applyTransform(
          ParDo.of(new BigtableDoFn[String, String](BigtableOptions, MaxPendingRequests) {
            override def asyncLookup(session: BigtableSession, input: String) =
              bigtableLookup(session, input)
          })
        )
        .map(checkResult)
        .sum
    }
  }

  // Async key value lookup for 52 millions from Bigtable with caching
  object AsyncCachingBigtableDoFnRead extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val cache = new AsyncLookupDoFn.CacheSupplier[String, String, String] {
        override def createCache() =
          CacheBuilder
            .newBuilder()
            .maximumSize(MillionElements)
            .build[String, String]()

        override def getKey(input: String) = input
      }

      sc.parallelize(Letters)
        .applyTransform(ParDo.of(new FillDoFn(1)))
        .flatMap(s => Seq.fill(MillionElements)(s))
        .applyTransform(
          ParDo.of(new BigtableDoFn[String, String](BigtableOptions, MaxPendingRequests, cache) {
            override def asyncLookup(session: BigtableSession, input: String) =
              bigtableLookup(session, input)
          })
        )
        .map(checkResult)
        .sum
    }
  }
}
