/*
 * Copyright 2016 Spotify AB.
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
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

private object VersionUtil {

  case class SemVer(major: Int, minor: Int, rev: Int, snapshot: String) extends Ordered[SemVer] {
    def compare(that: SemVer): Int = {
      implicit val revStringOrder = Ordering[String].reverse
      implicitly[Ordering[(Int, Int, Int, String)]]
        .compare(SemVer.unapply(this).get, SemVer.unapply(that).get)
    }
  }

  private val TIMEOUT = 3000
  private val url = "https://api.github.com/repos/spotify/scio/releases"
  private val pattern = """v?(\d+)\.(\d+).(\d+)(-SNAPSHOT)?""".r
  private val logger = LoggerFactory.getLogger(VersionUtil.getClass)

  private def getLatest: Option[String] = Try {
    val transport = new NetHttpTransport()
    val response = transport
      .createRequestFactory(new HttpRequestInitializer {
        override def initialize(request: HttpRequest) = {
          request.setConnectTimeout(TIMEOUT)
          request.setReadTimeout(TIMEOUT)
          request.setParser(new JsonObjectParser(new JacksonFactory))
        }
      })
      .buildGetRequest(new GenericUrl(url))
      .execute()
      .parseAs(classOf[java.util.List[Object]])
    val latest = response.iterator().next().asInstanceOf[java.util.Map[String, AnyRef]]
    latest.get("tag_name").toString
  }.toOption

  private def parseVersion(version: String): SemVer = {
    val m = pattern.findFirstMatchIn(version).get
    // higher value for no "-SNAPSHOT"
    val snapshot = if (m.group(4) != null) m.group(4) else ""
    SemVer(m.group(1).toInt, m.group(2).toInt, m.group(3).toInt, snapshot)
  }

  def checkVersion(current: String, latest: Option[String]): Seq[String] = {
    val b = mutable.Buffer.empty[String]
    val v1 = parseVersion(current)
    if (v1.snapshot == "-SNAPSHOT") {
      b.append(s"Using a SNAPSHOT version of Scio: $current")
    }
    latest.foreach { v =>
      val v2 = parseVersion(v)
      if (v2 > v1) {
        b.append(s"A newer version of Scio is available: $current -> $v")
      }
    }
    b
  }

  def checkVersion(): Unit =
    checkVersion(scioVersion, getLatest).foreach(logger.warn)

}
