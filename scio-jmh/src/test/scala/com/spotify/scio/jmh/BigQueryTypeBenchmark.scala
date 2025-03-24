/*
 * Copyright 2025 Spotify AB.
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

package com.spotify.scio.jmh

import com.spotify.scio.bigquery.types._
import org.joda.time.{Instant, LocalDateTime}
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

object BigQueryTypeBenchmark {
  case class NestedBqType(s: String)

  @BigQueryType.toTable
  case class BqTypeSimple(l: Long, s: String)

  @BigQueryType.toTable
  case class BqTypeNested(l: Long, s: String, n: NestedBqType)

  @BigQueryType.toTable
  case class BqTypeLogical(ldt: LocalDateTime, i: Instant)
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class BigQueryTypeBenchmark {
  import BigQueryTypeBenchmark._

  val BqtSimple = BigQueryType[BqTypeSimple]
  val BqtNested = BigQueryType[BqTypeNested]
  val BqtLogical = BigQueryType[BqTypeLogical]

  val CaseClassSimple = BqTypeSimple(10, "foo")
  val CaseClassNested = BqTypeNested(10, "foo", NestedBqType("bar"))
  val CaseClassLogical = BqTypeLogical(LocalDateTime.now(), Instant.now())

  val TableRowSimple = BqtSimple.toTableRow(CaseClassSimple)
  val TableRowNested = BqtNested.toTableRow(CaseClassNested)
  val TableRowLogical = BqtLogical.toTableRow(CaseClassLogical)

  val GrSimple = BqtSimple.toAvro(CaseClassSimple)
  val GrNested = BqtNested.toAvro(CaseClassNested)
  val GrLogical = BqtLogical.toAvro(CaseClassLogical)

  @Benchmark
  def bqtSimpleToTableRow(): Unit = BqtSimple.toTableRow(CaseClassSimple)

  @Benchmark
  def bqtSimpleToAvro(): Unit = BqtSimple.toAvro(CaseClassSimple)

  @Benchmark
  def bqtSimpleFromTableRow(): Unit = BqtSimple.fromTableRow(TableRowSimple)

  @Benchmark
  def bqtSimpleFromAvro(): Unit = BqtSimple.fromAvro(GrSimple)

  @Benchmark
  def bqtNestedToTableRow(): Unit = BqtNested.toTableRow(CaseClassNested)

  @Benchmark
  def bqtNestedToAvro(): Unit = BqtNested.toAvro(CaseClassNested)

  @Benchmark
  def bqtNestedFromTableRow(): Unit = BqtNested.fromTableRow(TableRowNested)

  @Benchmark
  def bqtNestedFromAvro(): Unit = BqtNested.fromAvro(GrNested)

  @Benchmark
  def bqtLogicalToTableRow(): Unit = BqtLogical.toTableRow(CaseClassLogical)

  @Benchmark
  def bqtLogicalToAvro(): Unit = BqtLogical.toAvro(CaseClassLogical)

  @Benchmark
  def bqtLogicalFromTableRow(): Unit = BqtLogical.fromTableRow(TableRowLogical)

  @Benchmark
  def bqtLogicalFromAvro(): Unit = BqtLogical.fromAvro(GrLogical)
}
