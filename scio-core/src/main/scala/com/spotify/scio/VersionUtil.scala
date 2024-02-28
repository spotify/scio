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

package com.spotify.scio

import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{GenericUrl, HttpRequest}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import org.apache.beam.sdk.util.ReleaseInfo
import org.apache.beam.sdk.{PipelineResult, PipelineRunner}
import org.slf4j.LoggerFactory

import scala.io.AnsiColor._
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Try

private[scio] object VersionUtil {

  private val oderSemVer: Ordering[SemVer] =
    Ordering.by(v => (v.major, v.minor, v.rev, v.suffix.toUpperCase()))

  case class SemVer(major: Int, minor: Int, rev: Int, suffix: String) extends Ordered[SemVer] {
    def compare(that: SemVer): Int = oderSemVer.compare(this, that)
  }

  private[this] val Timeout = 3000
  private[this] val Url = "https://api.github.com/repos/spotify/scio/releases"

  /**
   * example versions: version = "0.10.0-beta1+42-828dca9a-SNAPSHOT" version = "0.10.0-beta1"
   * version = "0.10.0-SNAPSHOT" version = "0.10-e135ed2-SNAPSHOT"
   */
  private[this] val Pattern = """^(0|[1-9]\d*)\.(0|[1-9]\d*)(?:\.(0|[1-9]\d*))?(?:-(.+))?$""".r
  private[this] val Logger = LoggerFactory.getLogger(this.getClass)

  private[this] val MessagePattern: (String, String) => String = (version, url) => s"""
       | $YELLOW>$BOLD Scio $version introduced some breaking changes in the API.$RESET
       | $YELLOW>$RESET Follow the migration guide to upgrade: $url.
       | $YELLOW>$RESET Scio provides automatic migration rules (See migration guide).
       |""".stripMargin
  private[this] val NewerVersionPattern: (String, String) => String = (current, v) => s"""
      | $YELLOW>$BOLD A newer version of Scio is available: $current -> $v$RESET
      | $YELLOW>$RESET Use `-Dscio.ignoreVersionWarning=true` to disable this check.
      |""".stripMargin

  private lazy val requestFactory = new NetHttpTransport()
    .createRequestFactory { (request: HttpRequest) =>
      request.setConnectTimeout(Timeout)
      request.setReadTimeout(Timeout)
    }

  private lazy val latest: Option[String] = Try {
    val response = requestFactory
      .buildGetRequest(new GenericUrl(Url))
      .setParser(new JsonObjectParser(GsonFactory.getDefaultInstance))
      .execute()
      .parseAs(classOf[java.util.List[java.util.Map[String, AnyRef]]])
    response.asScala
      .filterNot(_.get("prerelease").asInstanceOf[Boolean])
      .filterNot(_.get("draft").asInstanceOf[Boolean])
      .map(_.get("tag_name").asInstanceOf[String])
      .collectFirst { case tag if tag.head == 'v' => tag.tail }
  }.toOption.flatten

  private def parseVersion(version: String): SemVer = {
    val m = Pattern.findFirstMatchIn(version).get
    // use max value for pre-release if not defined
    val preRelease = Option(m.group(4)).getOrElse(new String(Array(Char.MaxValue)))
    val major = m.group(1).toInt
    val minor = m.group(2).toInt
    val tiny = Option(m.group(3)).map(_.toInt).getOrElse(0)
    SemVer(major, minor, tiny, preRelease)
  }

  private[scio] def ignoreVersionCheck: Boolean =
    sys.props.get("scio.ignoreVersionWarning").exists(_.trim == "true")

  private def migrationMessage(latest: SemVer): Option[String] = {
    val SemVer(major, minor, _, _) = latest
    val shortVersion = s"$major.$minor"
    val fullVersion = s"v$major.$minor.0"

    val url =
      s"https://spotify.github.io/scio/releases/migrations/$fullVersion-Migration-Guide.html"
    Try(requestFactory.buildGetRequest(new GenericUrl(url)).execute())
      .filter(_.getStatusCode == 200)
      .map(_ => MessagePattern(shortVersion, url))
      .toOption
  }

  def checkVersion(
    current: String,
    latestOverride: Option[String] = None,
    ignore: Boolean = ignoreVersionCheck
  ): Seq[String] =
    if (ignore) {
      Nil
    } else {
      val buffer = mutable.Buffer.empty[String]
      val currentVersion = parseVersion(current)
      if (currentVersion.suffix.contains("SNAPSHOT")) {
        buffer.append(s"Using a SNAPSHOT version of Scio: $current")
      }
      latestOverride.orElse(latest).foreach { v =>
        val latestVersion = parseVersion(v)
        if (latestVersion > currentVersion) {
          buffer.append(NewerVersionPattern(current, v))
          // breaking upgrade
          if (
            latestVersion.copy(rev = 0, suffix = "") > currentVersion.copy(rev = 0, suffix = "")
          ) {
            migrationMessage(latestVersion).foreach(buffer.append(_))
          }
        }
      }
      buffer.toSeq
    }

  def checkVersion(): Unit = checkVersion(BuildInfo.version).foreach(Logger.warn)

  def checkRunnerVersion(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Unit = {
    val name = runner.getSimpleName
    val version = ReleaseInfo.getReleaseInfo.getVersion
    if (version != BuildInfo.beamVersion) {
      Logger.warn(
        s"Mismatched version for $name, expected: ${BuildInfo.beamVersion}, actual: $version"
      )
    }
  }
}
