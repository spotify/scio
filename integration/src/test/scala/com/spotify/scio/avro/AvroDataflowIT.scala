/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.avro

import com.spotify.scio.{ContextAndArgs, ScioMetrics, ScioResult}
import com.spotify.scio.avro.dataflowit.ReflectRecord
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.avro._
import com.spotify.scio.testing.util.ItUtils
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Avro 1.12.2 rejects classes outside SERIALIZABLE_PACKAGES when decoding. The system property
// doesn't reach Dataflow workers, so these jobs check that scio trusts records on the workers.
// Each job shuffles its records so workers have to decode them.
class AvroDataflowIT extends AnyFlatSpec with Matchers {
  private val N = 100

  private def args(prefix: String, extra: String*): Array[String] =
    Array(
      s"--project=${ItUtils.project}",
      "--runner=DataflowRunner",
      "--region=europe-west1",
      s"--tempLocation=${ItUtils.gcpTempLocation(s"$prefix-tmp")}"
    ) ++ extra

  private def counter(r: ScioResult, name: String): Long =
    r.counter(ScioMetrics.counter(name)).committed.get

  private def deleteDir(dir: String): Unit = {
    val opts = PipelineOptionsFactory.create()
    opts.as(classOf[GcpOptions]).setProject(ItUtils.project)
    FileSystems.setDefaultPipelineOptions(opts)
    val files = FileSystems.`match`(s"$dir/**").metadata().asScala.map(_.resourceId())
    if (files.nonEmpty) {
      FileSystems.delete(files.asJava)
    }
  }

  private def account(i: Int): Account =
    Account
      .newBuilder()
      .setId(i)
      .setType("checking")
      .setName(s"account$i")
      .setAmount(i.toDouble)
      .build()

  "Avro on Dataflow" should "decode specific and generic records on workers" in {
    val dir = ItUtils.gcpTempLocation("avro-dataflow-it")
    try {
      val (sc1, _) = ContextAndArgs(args("avro-dataflow-it-write"))
      val accounts = sc1
        .parallelize(1 to N)
        .map(account)
        .groupBy(_.getId % 10)
        .flatMap(_._2)
      accounts.saveAsAvroFile(s"$dir/avro")
      accounts.saveAsParquetAvroFile(s"$dir/parquet")
      sc1.run().waitUntilDone().state shouldBe PipelineResult.State.DONE

      val (sc2, _) = ContextAndArgs(args("avro-dataflow-it-read"))
      sc2
        .avroFile[Account](s"$dir/avro/*.avro")
        .groupBy(_.getId % 10)
        .flatMap(_._2)
        .map(_ => ScioMetrics.counter("specificAvro").inc())
      sc2
        .avroFile(s"$dir/avro/*.avro", Account.getClassSchema)
        .map(_.get("id").asInstanceOf[Int])
        .groupBy(_ % 10)
        .map(_ => ScioMetrics.counter("genericAvro").inc())
      sc2
        .parquetAvroFile[Account](s"$dir/parquet/*.parquet")
        .groupBy(_.getId % 10)
        .flatMap(_._2)
        .map { a =>
          ScioMetrics.counter("specificParquet").inc()
          if (a.getName.isInstanceOf[String]) ScioMetrics.counter("parquetStrings").inc()
        }
      val r = sc2.run().waitUntilDone()
      r.state shouldBe PipelineResult.State.DONE
      counter(r, "specificAvro") shouldBe N
      counter(r, "genericAvro") shouldBe 10
      counter(r, "specificParquet") shouldBe N
      counter(r, "parquetStrings") shouldBe N
    } finally {
      deleteDir(dir)
    }
  }

  it should "decode reflect records on workers with --avroTrustedPackages" in {
    val (sc, _) = ContextAndArgs(
      args(
        "avro-dataflow-it-reflect",
        "--avroTrustedPackages=com.spotify.scio.avro.dataflowit"
      )
    )
    implicit val coder: Coder[ReflectRecord] = Coder.beam(AvroCoder.of(classOf[ReflectRecord]))
    sc.parallelize(1 to N)
      .map(i => new ReflectRecord(i, s"name$i"))
      .groupBy(_.id % 10)
      .flatMap(_._2)
      .map(_ => ScioMetrics.counter("reflect").inc())
    val r = sc.run().waitUntilDone()
    r.state shouldBe PipelineResult.State.DONE
    counter(r, "reflect") shouldBe N
  }
}

// Reflect records need a no-arg constructor. com.spotify.scio.avro.dataflowit is not in Avro's
// default SERIALIZABLE_PACKAGES, so workers only decode this with --avroTrustedPackages.
package dataflowit {
  class ReflectRecord(var id: Int, var name: String) {
    def this() = this(0, null)
  }
}
