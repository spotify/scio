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

package com.spotify.scio.parquet

import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

private[parquet] object GcsConnectorUtil {
  def setCredentials(job: Job): Unit =
    // These are needed since `FileInputFormat.setInputPaths` validates paths locally and
    // requires the user's GCP credentials.
    sys.env.get("GOOGLE_APPLICATION_CREDENTIALS") match {
      case Some(json) =>
        job.getConfiguration
          .set("fs.gs.auth.service.account.json.keyfile", json)
      case None =>
        // Client id/secret of Google-managed project associated with the Cloud SDK
        job.getConfiguration
          .setBoolean("fs.gs.auth.service.account.enable", false)
        job.getConfiguration.set("fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
        job.getConfiguration
          .set("fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")
    }

  def unsetCredentials(job: Job): Unit = {
    job.getConfiguration.unset("fs.gs.auth.service.account.json.keyfile")
    job.getConfiguration.unset("fs.gs.auth.service.account.enable")
    job.getConfiguration.unset("fs.gs.auth.client.id")
    job.getConfiguration.unset("fs.gs.auth.client.secret")
  }

  def setInputPaths(sc: ScioContext, job: Job, path: String): Unit = {
    // This is needed since `FileInputFormat.setInputPaths` validates paths locally and requires
    // the user's GCP credentials.
    GcsConnectorUtil.setCredentials(job)

    FileInputFormat.setInputPaths(job, path)

    // It will interfere with credentials in Dataflow workers
    if (!ScioUtil.isLocalRunner(sc.options.getRunner)) {
      GcsConnectorUtil.unsetCredentials(job)
    }
  }
}
