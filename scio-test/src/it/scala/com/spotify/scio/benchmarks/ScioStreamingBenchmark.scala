/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.benchmarks

import java.util.UUID

import com.google.api.services.dataflow.model.Job
import com.google.common.reflect.ClassPath
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Streaming benchmark jobs, restarted daily and polled for metrics every hour.
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioStreamingBenchmark.scala so
 * that it can run with past Scio releases.
 */
object ScioStreamingBenchmark {
  import DataflowProvider._
  import ScioBenchmarkSettings._

  // Launch the streaming benchmark jobs and cancel old jobs
  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", DefaultProjectId)
    val timestamp = DateTimeFormat
      .forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val prefix = s"ScioStreamingBenchmark-$name-$timestamp"

    cancelCurrentJobs(projectId)

    Benchmarks
      .filter(_.name.matches(regex))
      .map(_.run(projectId, prefix, commonArgs("n1-highmem-8")))
  }

  private val Benchmarks = ClassPath
    .from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName
      .matches("com\\.spotify\\.scio\\.benchmarks\\.ScioStreamingBenchmark\\$[\\w]+\\$"))
    .flatMap { ci =>
      val cls = ci.load()
      if (classOf[StreamingBenchmark] isAssignableFrom cls) {
        Some(cls.newInstance().asInstanceOf[StreamingBenchmark])
      } else {
        None
      }
    }

  private val CancelledState = "JOB_STATE_CANCELLED"

  /** Cancel any currently running streaming benchmarks before spawning new ones */
  private def cancelCurrentJobs(projectId: String): Unit = {
    val jobs = Dataflow.projects().jobs()

    Option(jobs.list(projectId).setFilter("ACTIVE").execute().getJobs).foreach { activeJobs =>
      activeJobs.asScala.foreach { job =>
        if (job.getName.toLowerCase.startsWith("sciostreamingbenchmark")) {
          PrettyPrint.print("CancelCurrentJobs", s"Stopping job.... ${job.getName}")

          jobs
            .update(
              projectId,
              job.getId,
              new Job().setProjectId(projectId).setId(job.getId).setRequestedState(CancelledState)
            )
            .execute()
        }
      }
    }
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================
  private val M = 10000
  private val K = 1000

  case class Nested(value: Array[String])
  case class SomeObject(key: String, value: Float, list: List[Long], nested: Map[String, Nested])
  case class CompoundKey(k1: String, k2: Long)

  object StreamingExample extends StreamingBenchmark {
    override def run(sc: ScioContext): Unit = {
      val pubsubOut = outputTopic(sc)

      val sideInput = randomUUIDs(sc, perMinute = K).asListSideInput

      randomUUIDs(sc, perMinute = M)
        .map { uuid =>
          val nested = Nested(uuid.split(""))
          (
            CompoundKey(uuid.charAt(0).toString, Random.nextInt(50000)),
            SomeObject(uuid,
                       Random.nextFloat(),
                       (1L to 10000L).toList,
                       uuid.split("").map((_, nested)).toMap)
          )
        }
        .groupByKey
        .withSideInputs(sideInput)
        .flatMap {
          case ((key, grp), ctx) =>
            Some(
              (key.copy(k2 = key.k2 + Random.nextInt(50000)),
               (
                 key.k1,
                 ctx(sideInput).take(50),
                 grp.map(obj => obj.copy(key = s"${obj.key}2", list = obj.list.reverse))
               )))
        }
        .toSCollection
        .minByKey(Ordering.by(_._2.size))
        .map(_ => 1) // keep message size small to minimize pub/sub cost
        .saveAsPubsub(pubsubOut)
    }
  }

  private def randomUUIDs(sc: ScioContext, perMinute: Int): SCollection[String] =
    sc.customInput(
        "createRandomUUIDs",
        GenerateSequence
          .from(0)
          .withRate(perMinute, org.joda.time.Duration.standardSeconds(60))
      )
      .withFixedWindows(Duration.standardSeconds(60), Duration.ZERO)
      .map(_ => UUID.randomUUID().toString)
}

abstract class StreamingBenchmark extends ScioJob {
  import ScioBenchmarkSettings.CircleCI

  val name: String = this.getClass.getSimpleName.replaceAll("\\$$", "")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(projectId: String, prefix: String, args: Array[String]): (String, ScioResult) = {
    val username = CoreSysProps.User.value
    val buildNum = CircleCI.map(_.buildNum).getOrElse(-1L)
    val gitHash = CircleCI.map(_.gitHash).getOrElse("none")

    val (sc, _) = ContextAndArgs(args)
    sc.setAppName(name)
    sc.setJobName(s"$prefix-$name-$buildNum-$gitHash-$username".toLowerCase())
    sc.optionsAs[GcpOptions].setProject(projectId)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    run(sc)

    (name, sc.close())
  }

  def outputTopic(sc: ScioContext): String =
    s"projects/${sc.optionsAs[GcpOptions].getProject}/topics/StreamingBenchmark-$name"
}

object ScioStreamingBenchmarkMetrics {

  import DataflowProvider._
  import ScioBenchmarkSettings._

  // Triggered once an hour to poll job metrics from currently running streaming benchmarks
  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val projectId = argz.getOrElse("project", DefaultProjectId)
    val jobNamePattern =
      s"sciostreamingbenchmark-$name-\\d+-([a-zA-Z0-9]+)-(-?\\d+)-([a-zA-Z0-9]+)-\\w+".r

    val jobs = Dataflow.projects().jobs().list(projectId)

    val hourlyMetrics =
      Option(jobs.setFilter("ACTIVE").execute().getJobs)
        .map { activeJobs =>
          activeJobs.asScala.flatMap { job =>
            job.getName match {
              case jobNamePattern(name, buildNum, gitHash) =>
                Some(
                  BenchmarkResult.streaming(
                    Instant.now(),
                    name,
                    buildNum.toLong,
                    gitHash,
                    job.getCreateTime,
                    Dataflow.projects().jobs().getMetrics(projectId, job.getId).execute()
                  ))

              case _ => None
            }
          }
        }
        .getOrElse(List())

    new DatastoreLogger() {
      override def dsKeyId(benchmark: BenchmarkResult): String = {
        val startTime = ISODateTimeFormat.dateTimeParser().parseLocalDateTime(benchmark.startTime)
        val hourOffset = Hours
          .hoursBetween(
            startTime,
            new LocalDateTime(DateTimeZone.UTC)
          )
          .getHours

        s"${benchmark.buildNum}[+${"%02d".format(hourOffset)}h]"
      }
    }.log(hourlyMetrics)
  }
}
