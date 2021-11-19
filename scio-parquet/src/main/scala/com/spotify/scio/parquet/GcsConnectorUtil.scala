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
import com.google.cloud.hadoop.util.AccessTokenProvider
import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import java.io.File
import java.util.Locale
import scala.util.Try

private[parquet] object GcsConnectorUtil {
  def setCredentials(job: Job): Unit = {
    // These are needed since `FileInputFormat.setInputPaths` validates paths locally and
    // requires the user's GCP credentials.
    lazy val applicationDefault = Try(GoogleCredentials.getApplicationDefault())

    sys.env
      .get("GOOGLE_APPLICATION_CREDENTIALS")
      .filter(_.nonEmpty)
      .orElse {
        applicationDefault.toOption match {
          case Some(_: ServiceAccountCredentials) => getWellKnownCredentialFile.map(_.toString)
          case _                                  => None
        }
      } match {
      case Some(serviceAccountKeyFile) =>
        job.getConfiguration.set("fs.gs.auth.service.account.json.keyfile", serviceAccountKeyFile)
      case _ if applicationDefault.isSuccess => // ADC exists but isn't a service account
        job.getConfiguration.set(
          "fs.gs.auth.access.token.provider.impl",
          "com.spotify.scio.parquet.ApplicationDefaultTokenProvider"
        )
      case _ =>
        throw new IllegalStateException(
          "No valid Google credentials were found. " +
            "Check that application default is set."
        )
    }
  }

  def unsetCredentials(job: Job): Unit = {
    job.getConfiguration.unset("fs.gs.auth.service.account.json.keyfile")
    job.getConfiguration.unset("fs.gs.auth.access.token.provider.impl")
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

class ApplicationDefaultTokenProvider() extends AccessTokenProvider {
  private lazy val adc = GoogleCredentials.getApplicationDefault()
  private var conf: Option[Configuration] = None

  override def getAccessToken: AccessTokenProvider.AccessToken = {
    val gToken = Option(adc.getAccessToken).getOrElse { adc.refresh(); adc.getAccessToken }
    new AccessTokenProvider.AccessToken(gToken.getTokenValue, gToken.getExpirationTime.getTime)
  }
  override def refresh(): Unit = adc.refresh()
  override def setConf(c: Configuration): Unit = conf = Some(c)
  override def getConf: Configuration = conf.orNull
}
