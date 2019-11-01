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

// Ported from
// https://github.com/google/google-api-java-client/blob/master/google-api-client/src/main/java/com/google/api/client/googleapis/auth/oauth2/DefaultCredentialProvider.java

import java.io.File
import java.util.Locale

object BuildCredentials {
  private val CREDENTIAL_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS"
  private val CLOUDSDK_CONFIG_DIRECTORY = "gcloud"
  private val WELL_KNOWN_CREDENTIALS_FILE =
    "application_default_credentials.json"

  def exists: Boolean =
    runningUsingEnvironmentVariable || runningUsingWellKnownFile

  private def runningUsingEnvironmentVariable: Boolean = {
    val credentialsPath = sys.env.getOrElse(CREDENTIAL_ENV_VAR, null)
    if (credentialsPath == null || credentialsPath.length == 0) {
      false
    } else {
      val credentialsFile = new File(credentialsPath)
      fileExists(credentialsFile)
    }
  }

  private def runningUsingWellKnownFile: Boolean = {
    val os = sys.props.getOrElse("os.name", "").toLowerCase(Locale.US)
    val cloudConfigPath = if (os.contains("windows")) {
      val appDataPath = new File(sys.env("APPDATA"))
      new File(appDataPath, CLOUDSDK_CONFIG_DIRECTORY)
    } else {
      val configPath = new File(sys.props.getOrElse("user.home", ""), ".config")
      new File(configPath, CLOUDSDK_CONFIG_DIRECTORY)
    }
    val credentialFilePath =
      new File(cloudConfigPath, WELL_KNOWN_CREDENTIALS_FILE)
    fileExists(credentialFilePath)
  }

  private def fileExists(file: File): Boolean =
    file.exists() && !file.isDirectory
}
