/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio.examples

import com.spotify.scio.util.ScioUtil
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

object RunPreReleaseIT {

  implicit private val ec: ExecutionContext = ExecutionContext.global

  private val defaultArgs = Array(
    "--runner=DataflowRunner",
    "--project=data-integration-test",
    "--region=us-central1",
    "--tempLocation=gs://dataflow-tmp-us-central1/gha"
  )

  private val log = LoggerFactory.getLogger(getClass)

  def main(cmdLineArgs: Array[String]): Unit = {
    val (_, args) = ContextAndArgs(cmdLineArgs)
    val runId = args("runId")

    try {
      val jobs = List(parquet(runId), avro(runId), smb(runId), bigquery())
      Await.result(Future.sequence(jobs), 1.hour)
    } catch {
      case t: Throwable =>
        throw new RuntimeException("At least one Dataflow job failed", t)
    }
  }

  private def avro(runId: String): Future[Unit] = {
    import com.spotify.scio.examples.extra.AvroExample

    val out1 = gcsPath[AvroExample.type]("specificOut", runId)
    val out2 = gcsPath[AvroExample.type]("specificIn", runId)

    log.info("Starting Avro write tests... ")
    val write = invokeJob[AvroExample.type]("--method=specificOut", s"--output=$out1")
    write.flatMap { _ =>
      log.info("Starting Avro read tests... ")
      invokeJob[AvroExample.type]("--method=specificIn", s"--input=$out1/*", s"--output=$out2")
    }
  }

  private def parquet(runId: String): Future[Unit] = {
    import com.spotify.scio.examples.extra.ParquetExample

    val out1 = gcsPath[ParquetExample.type]("avroOut", runId)
    val out2 = gcsPath[ParquetExample.type]("typedIn", runId)
    val out3 = gcsPath[ParquetExample.type]("avroSpecificIn", runId)
    val out4 = gcsPath[ParquetExample.type]("avroGenericIn", runId)
    val out5 = gcsPath[ParquetExample.type]("exampleOut", runId)
    val out6 = gcsPath[ParquetExample.type]("exampleIn", runId)

    log.info("Starting Parquet write tests... ")
    val writes = List(
      invokeJob[ParquetExample.type]("--method=avroOut", s"--output=$out1"),
      invokeJob[ParquetExample.type]("--method=exampleOut", s"--output=$out5")
    )

    Future.sequence(writes).flatMap { _ =>
      log.info("Starting Parquet read tests... ")
      val reads = List(
        invokeJob[ParquetExample.type]("--method=typedIn", s"--input=$out1/*", s"--output=$out2"),
        invokeJob[ParquetExample.type](
          "--method=avroSpecificIn",
          s"--input=$out1/*",
          s"--output=$out3"
        ),
        invokeJob[ParquetExample.type](
          "--method=avroGenericIn",
          s"--input=$out1/*",
          s"--output=$out4"
        ),
        invokeJob[ParquetExample.type](
          "--method=avroGenericIn",
          s"--input=$out1/*",
          s"--output=$out4"
        ),
        invokeJob[ParquetExample.type](
          "--method=exampleIn",
          s"--input=$out5/*",
          s"--output=$out6"
        )
      )
      Future.sequence(reads).map(_ => ())
    }
  }

  private def smb(runId: String): Future[Unit] = {
    import com.spotify.scio.examples.extra.{
      SortMergeBucketJoinExample,
      SortMergeBucketTransformExample,
      SortMergeBucketWriteExample
    }
    val out1 = gcsPath[SortMergeBucketWriteExample.type]("users", runId)
    val out2 = gcsPath[SortMergeBucketWriteExample.type]("accounts", runId)

    log.info("Starting SMB write tests... ")
    val write = invokeJob[SortMergeBucketWriteExample.type](s"--users=$out1", s"--accounts=$out2")
    write.flatMap { _ =>
      log.info("Starting SMB read tests... ")
      val readJobs = List(
        invokeJob[SortMergeBucketJoinExample.type](
          s"--users=$out1",
          s"--accounts=$out2",
          s"--output=${gcsPath[SortMergeBucketJoinExample.type]("join", runId)}"
        ),
        invokeJob[SortMergeBucketTransformExample.type](
          s"--users=$out1",
          s"--accounts=$out2",
          s"--output=${gcsPath[SortMergeBucketTransformExample.type]("transform", runId)}"
        )
      )
      Future.sequence(readJobs).map(_ => ())
    }
  }

  private def bigquery(): Future[Unit] = {
    import com.spotify.scio.examples.extra.{TypedBigQueryTornadoes, TypedStorageBigQueryTornadoes}
    log.info("Starting BigQuery tests... ")
    val jobs = List(
      invokeJob[TypedStorageBigQueryTornadoes.type](
        s"--output=data-integration-test:gha_it_us.typed_storage"
      ),
      invokeJob[TypedBigQueryTornadoes.type](
        s"--output=data-integration-test:gha_it_us.typed_row"
      )
    )
    Future.sequence(jobs).map(_ => ())
  }

  private def invokeJob[T: ClassTag](args: String*): Future[Unit] = {
    val cls = ScioUtil.classOf[T]
    val jobObjName = cls.getName.replaceAll("\\$$", "")
    val pipelines = Class
      .forName(jobObjName)
      .getMethod("pipeline", classOf[Array[String]])
      .invoke(null, defaultArgs ++ Array(args: _*))
      .asInstanceOf[ScioContext]

    val job = pipelines.run()
    Future(job.waitUntilDone())
  }

  private def gcsPath[T: ClassTag](methodName: String, runId: String): String =
    s"gs://data-integration-test-us/${ScioUtil.classOf[T].getSimpleName}/$methodName/$runId"
}
