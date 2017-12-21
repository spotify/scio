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

import com.google.bigtable.v2.{ReadRowsRequest, RowFilter, RowSet}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.scanner.FlatRow
import com.google.cloud.bigtable.grpc.{BigtableInstanceName, BigtableSession}
import com.google.common.cache.CacheBuilder
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigtable._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV

import scala.util.{Failure, Success}

object BigtableBenchmark {
  val projectId: String = "scio-playground"
  val instanceId: String = "side-input-test"
  val tableId: String = "side-input-test"

  val bigtableOptions: BigtableOptions = new BigtableOptions.Builder()
    .setProjectId(projectId)
    .setInstanceId(instanceId)
    .setUserAgent("bigtable-test")
    .build()

  val familyName: String = "side"
  val columnQualifier: ByteString = ByteString.copyFromUtf8("value")

  val readRowsRequestTemplate: ReadRowsRequest = ReadRowsRequest.newBuilder()
    .setTableName(new BigtableInstanceName(projectId, instanceId).toTableNameStr(tableId))
    .setFilter(RowFilter.newBuilder()
      .setFamilyNameRegexFilter(familyName)
      .setColumnQualifierRegexFilter(columnQualifier))
    .setRowsLimit(1L)
    .build()

  val lowerLetters: Seq[String] = (0 until 26).map('a'.toInt + _).map(_.toChar.toString)
  val upperLetters: Seq[String] = lowerLetters.map(_.toUpperCase)
  val letters: Seq[String] = lowerLetters ++ upperLetters

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

  def bigtableLookup(session: BigtableSession, input: String): ListenableFuture[String] =
    if (input.endsWith("0000000000")) {
      Futures.immediateFailedFuture(new RuntimeException(input))
    } else {
      val s = input
      val key = ByteString.copyFromUtf8(s"key-$s")
      val expected = ByteString.copyFromUtf8(s"val-$s")
      val request = readRowsRequestTemplate.toBuilder
        .setRows(RowSet.newBuilder().addRowKeys(key).build())
        .build()
      val future = session.getDataClient.readFlatRowsAsync(request)
      Futures.transform(future,
        new com.google.common.base.Function[java.util.List[FlatRow], String] {
          override def apply(input: java.util.List[FlatRow]) = {
            val result = input
            assert(result.size() == 1)
            val cells = result.get(0).getCells
            assert(result.get(0).getCells.size() == 1)
            val value = cells.get(0).getValue
            assert(value == expected)
            value.toStringUtf8
          }
        })
    }

  def checkResult(kv: KV[String, BigtableDoFn.Try[String]]): (Int, Int) =
    kv.getValue.asScala match {
      case Success(value) =>
        require(kv.getKey == value.substring(4))
        (1, 0)
      case Failure(exception) =>
        require(kv.getKey.endsWith("0000000000"))
        require(kv.getKey == exception.getMessage)
        (0, 1)
    }
}

// Generate 52 million key value pairs
object BigtableWrite {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.ensureTables(bigtableOptions, Map(tableId -> List(familyName)))
    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .map { s =>
        val key = ByteString.copyFromUtf8(s"key-$s")
        val value = ByteString.copyFromUtf8(s"val-$s")
        val m = Mutations.newSetCell(familyName, columnQualifier, value, 0L)
        (key, Iterable(m))
      }
      .saveAsBigtable(bigtableOptions, tableId)
    sc.close()
  }
}

// Async key value lookup
object AsyncBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .applyTransform(ParDo.of(new BigtableDoFn[String, String](bigtableOptions, 10000) {
        override def asyncLookup(session: BigtableSession, input: String) =
          bigtableLookup(session, input)
      }))
      .map(checkResult)
      .sum
    sc.close()
  }
}

// Async key value lookup with caching
object AsyncCachingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val cache = new BigtableDoFn.CacheSupplier[String, String, String] {
      override def createCache() = CacheBuilder.newBuilder()
        .maximumSize(1000000)
        .build[String, String]()
      override def getKey(input: String) = input
    }

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1)))
      .flatMap(s => Seq.fill(1000000)(s))
      .applyTransform(ParDo.of(new BigtableDoFn[String, String](bigtableOptions, 10000, cache) {
        override def asyncLookup(session: BigtableSession, input: String) =
          bigtableLookup(session, input)
      }))
      .map(checkResult)
      .sum
    sc.close()
  }
}

// Blocking key value lookup
object BlockingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .applyTransform(ParDo.of(new BigtableDoFn[String, String](bigtableOptions) {
        override def asyncLookup(session: BigtableSession, input: String) = {
          val output = bigtableLookup(session, input).get()
          Futures.immediateFuture(output)
        }
      }))
      .map(checkResult)
      .sum
    sc.close()
  }
}

// Blocking key value lookup with caching
object BlockingCachingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val cache = new BigtableDoFn.CacheSupplier[String, String, String] {
      override def createCache() = CacheBuilder.newBuilder()
        .maximumSize(1000000)
        .build[String, String]()
      override def getKey(input: String) = input
    }

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1)))
      .flatMap(s => Seq.fill(1000000)(s))
      .applyTransform(ParDo.of(new BigtableDoFn[String, String](bigtableOptions) {
        override def asyncLookup(session: BigtableSession, input: String) = {
          val output = bigtableLookup(session, input).get()
          Futures.immediateFuture(output)
        }
      }))
      .map(checkResult)
      .sum
    sc.close()
  }
}
