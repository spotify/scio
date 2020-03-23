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
import com.google.api.client.http.{GenericUrl, HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import org.apache.beam.sdk.util.ReleaseInfo
import org.apache.beam.sdk.{PipelineResult, PipelineRunner}
import org.slf4j.LoggerFactory

import scala.io.AnsiColor._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

private[scio] object VersionUtil {
  case class SemVer(major: Int, minor: Int, rev: Int, suffix: String) extends Ordered[SemVer] {
    def compare(that: SemVer): Int =
      Ordering[(Int, Int, Int, String)].compare(SemVer.unapply(this).get, SemVer.unapply(that).get)
  }

  private[this] val Timeout = 3000
  private[this] val Url = "https://api.github.com/repos/spotify/scio/releases"
  private[this] val Pattern = """v?(\d+)\.(\d+).(\d+)(-\w+)?""".r
  private[this] val Logger = LoggerFactory.getLogger(this.getClass)

  private[this] val MessagePattern: (String, String) => String = (version, url) => s"""
       | $YELLOW>$BOLD Scio $version introduced some breaking changes in the API.$RESET
       | $YELLOW>$RESET Follow the migration guide to upgrade: $url.
       | $YELLOW>$RESET Scio provides automatic migration rules (See migration guide).
      """.stripMargin
  private[this] val NewerVersionPattern: (String, String) => String = (current, v) => s"""
      | $YELLOW>$BOLD A newer version of Scio is available: $current -> $v$RESET
      | $YELLOW>$RESET Use `-Dscio.ignoreVersionWarning=true` to disable this check.$RESET
      |""".stripMargin

  private lazy val latest: Option[String] = Try {
    val transport = new NetHttpTransport()
    val response = transport
      .createRequestFactory(new HttpRequestInitializer {
        override def initialize(request: HttpRequest): Unit = {
          request.setConnectTimeout(Timeout)
          request.setReadTimeout(Timeout)
          request.setParser(new JsonObjectParser(new JacksonFactory))

          ()
        }
      })
      .buildGetRequest(new GenericUrl(Url))
      .execute()
      .parseAs(classOf[java.util.List[java.util.Map[String, AnyRef]]])
    response.asScala
      .filter(node => !node.get("prerelease").asInstanceOf[Boolean])
      .find(node => !node.get("draft").asInstanceOf[Boolean])
      .map(latestNode => latestNode.get("tag_name").asInstanceOf[String])
  }.toOption.flatten

  private def parseVersion(version: String): SemVer = {
    val m = Pattern.findFirstMatchIn(version).get
    // higher value for no "-SNAPSHOT"
    val snapshot = if (m.group(4) != null) m.group(4).toUpperCase else "\uffff"
    SemVer(m.group(1).toInt, m.group(2).toInt, m.group(3).toInt, snapshot)
  }

  private[scio] def ignoreVersionCheck: Boolean =
    Option(System.getProperty("scio.ignoreVersionWarning")).exists(_.trim == "true")

  private def messages(current: SemVer, latest: SemVer): Option[String] = (current, latest) match {
    case (SemVer(0, minor, _, _), SemVer(0, 7, _, _)) if minor < 7 =>
      Some(
        MessagePattern("0.7", "https://spotify.github.io/scio/migrations/v0.7.0-Migration-Guide")
      )
    case (SemVer(0, minor, _, _), SemVer(0, 8, _, _)) if minor < 8 =>
      Some(
        MessagePattern("0.8", "https://spotify.github.io/scio/migrations/v0.8.0-Migration-Guide")
      )
    case _ => None
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
      val v1 = parseVersion(current)
      if (v1.suffix == "-SNAPSHOT") {
        buffer.append(s"Using a SNAPSHOT version of Scio: $current")
      }
      latestOverride.orElse(latest).foreach { v =>
        val v2 = parseVersion(v)
        if (v2 > v1) {
          buffer.append(NewerVersionPattern(current, v))
          messages(v1, v2).foreach(buffer.append(_))
        }
      }
      buffer.toSeq
    }

  def checkVersion(): Unit = checkVersion(BuildInfo.version).foreach(Logger.warn)

  def checkRunnerVersion(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Unit = {
    val name = runner.getSimpleName
    val version = ReleaseInfo.getReleaseInfo.getVersion
    require(
      version == BuildInfo.beamVersion,
      s"Mismatched version for $name, expected: ${BuildInfo.beamVersion}, actual: $version"
    )
  }
}
