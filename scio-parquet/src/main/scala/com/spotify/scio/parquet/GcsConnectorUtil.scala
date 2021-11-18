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

import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Locale
import scala.util.Try

private[parquet] object GcsConnectorUtil {
  private lazy val log = LoggerFactory.getLogger(getClass)

  def setCredentials(job: Job): Unit = {
    // These are needed since `FileInputFormat.setInputPaths` validates paths locally and
    // requires the user's GCP credentials.
    val keyFile = sys.env
      .get("GOOGLE_APPLICATION_CREDENTIALS")
      .filter(_.nonEmpty)
      .orElse {
        Try(GoogleCredentials.getApplicationDefault()).toOption match {
          case Some(_: ServiceAccountCredentials) => getWellKnownCredentialFile.map(_.toString)
          case _ => None // ADC was not set, or was a personal user credential
        }
      }

    keyFile match {
      case Some(path) => job.getConfiguration.set("fs.gs.auth.service.account.json.keyfile", path)
      case None =>
        log.warn(
          "Application default credentials could not be resolved, using default Cloud SDK credentials"
        )
        // Client id/secret of Google-managed project associated with the Cloud SDK
        job.getConfiguration
          .setBoolean("fs.gs.auth.service.account.enable", false)
        job.getConfiguration.set("fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
        job.getConfiguration
          .set("fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")
    }
  }

  def unsetCredentials(job: Job): Unit = {
    job.getConfiguration.unset("fs.gs.auth.service.account.json.keyfile")
    job.getConfiguration.unset("fs.gs.auth.service.account.enable")
    job.getConfiguration.unset("fs.gs.auth.service.account.enable")
    job.getConfiguration.unset("fs.gs.auth.null.enable")
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

  // Adapted from com.google.auth.oauth2.DefaultCredentialsProvider
  private def getWellKnownCredentialFile: Option[File] = {
    val os = sys.props.getOrElse("os.name", "").toLowerCase(Locale.US)
    val cloudRootPath = if (os.contains("windows")) {
      new File(sys.env("APPDATA"))
    } else {
      new File(sys.props.getOrElse("user.home", ""), ".config")
    }
    val credentialFilePath =
      new File(cloudRootPath, "gcloud/application_default_credentials.json")

    if (credentialFilePath.exists()) Some(credentialFilePath) else None
  }
}
