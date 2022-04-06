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

package com.spotify.scio.bigquery.client

import java.io.IOException

import com.google.api.services.bigquery.model.Job
import com.spotify.scio.bigquery.client.BigQuery.Client
import org.apache.commons.io.FileUtils
import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

private[client] object JobOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private val PeriodFormatter = new PeriodFormatterBuilder()
    .appendHours()
    .appendSuffix("h")
    .appendMinutes()
    .appendSuffix("m")
    .appendSecondsWithOptionalMillis()
    .appendSuffix("s")
    .toFormatter

  private def logJobStatistics(bqJob: BigQueryJob, job: Job): Unit = {
    val stats = job.getStatistics
    Logger.info(s"${bqJob.show} completed")

    bqJob match {
      case _: ExtractJob =>
        val destinationFileCount =
          stats.getExtract.getDestinationUriFileCounts.asScala.reduce(_ + _)

        Logger.info(s"Total destination file count: $destinationFileCount")

      case _: LoadJob =>
        val inputFileBytes = FileUtils.byteCountToDisplaySize(stats.getLoad.getInputFileBytes)
        val outputBytes = FileUtils.byteCountToDisplaySize(stats.getLoad.getOutputBytes)
        val outputRows = stats.getLoad.getOutputRows
        Logger.info(
          s"Input file bytes: $inputFileBytes, output bytes: $outputBytes, " +
            s"output rows: $outputRows"
        )

      case queryJob: QueryJob =>
        Logger.info(s"Query: `${queryJob.query}`")
        val bytes = FileUtils.byteCountToDisplaySize(stats.getQuery.getTotalBytesProcessed)
        val cacheHit = stats.getQuery.getCacheHit
        Logger.info(s"Total bytes processed: $bytes, cache hit: $cacheHit")
    }

    val elapsed = PeriodFormatter.print(new Period(stats.getEndTime - stats.getCreationTime))
    val pending = PeriodFormatter.print(new Period(stats.getStartTime - stats.getCreationTime))
    val execution = PeriodFormatter.print(new Period(stats.getEndTime - stats.getStartTime))
    Logger.info(s"Elapsed: $elapsed, pending: $pending, execution: $execution")
  }
}

final private[client] class JobOps(client: Client) {
  import JobOps._

  /** Wait for all jobs to finish. */
  def waitForJobs(jobs: BigQueryJob*): Unit = {
    val numTotal = jobs.size
    var pendingJobs = jobs.flatMap { job =>
      job.jobReference match {
        case Some(reference) => Some((job, reference))
        case None            => None
      }
    }

    while (pendingJobs.nonEmpty) {
      val remainingJobs = pendingJobs.filter { case (bqJob, jobReference) =>
        val jobId = jobReference.getJobId
        try {
          val poll = client.execute(
            _.jobs()
              .get(client.project, jobId)
              .setLocation(jobReference.getLocation)
          )
          val error = poll.getStatus.getErrorResult
          if (error != null) {
            throw new RuntimeException(s"${bqJob.show} failed with error: $error")
          }
          if (poll.getStatus.getState == "DONE") {
            logJobStatistics(bqJob, poll)
            false
          } else {
            true
          }
        } catch {
          case e: IOException =>
            Logger.warn(s"BigQuery request failed: id: $jobId, error: $e")
            true
        }
      }

      pendingJobs = remainingJobs
      val numDone = numTotal - pendingJobs.size
      Logger.info(s"Job: $numDone out of $numTotal completed")
      if (pendingJobs.nonEmpty) {
        Thread.sleep(10000)
      }
    }
  }
}
