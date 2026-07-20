/*
 * Copyright 2026 Spotify AB.
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.StringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GcsConnectorUtilTest extends AnyFlatSpec with Matchers {

  "setCredentials" should "set SERVICE_ACCOUNT_JSON_KEYFILE auth type for service account credentials" in {
    val conf = ParquetConfiguration.empty()
    sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").foreach { _ =>
      GcsConnectorUtil.setCredentials(conf)
      val authType = conf.get("fs.gs.auth.type")
      authType should (
        equal("SERVICE_ACCOUNT_JSON_KEYFILE") or equal("ACCESS_TOKEN_PROVIDER")
      )
    }
  }

  it should "set auth type and clear it on unset" in {
    val conf = ParquetConfiguration.empty()
    GcsConnectorUtil.setCredentials(conf)
    conf.get("fs.gs.auth.type") should not be null

    GcsConnectorUtil.unsetCredentials(conf)
    conf.get("fs.gs.auth.type") shouldBe null
  }

  private def getInputPaths(sc: ScioContext, path: String): Array[String] = {
    val conf = ParquetConfiguration.empty()
    GcsConnectorUtil.setInputPaths(sc, conf, path)
    val raw = conf.get(FileInputFormat.INPUT_DIR)
    StringUtils.split(raw).map(StringUtils.unEscapeString(_))
  }

  "GcsConnectorUtil" should "preserve URI scheme for a single GCS path" in {
    val sc = ScioContext()
    val path = "gs://bucket/path/to/*.parquet"
    val paths = getInputPaths(sc, path)
    paths should have length 1
    paths(0) shouldBe path
  }

  it should "preserve URI scheme for multiple comma-separated GCS paths" in {
    val sc = ScioContext()
    val uris = (0 until 24).map(h => f"gs://bucket/data/2026-04-08T$h%02d/*.parquet")
    val path = uris.mkString(",")
    val paths = getInputPaths(sc, path)
    paths should have length 24
    paths.zip(uris).foreach { case (actual, expected) =>
      actual shouldBe expected
    }
  }

  it should "preserve gs:// scheme and not collapse double slashes" in {
    val sc = ScioContext()
    val path = "gs://bucket-a/path/*.parquet,gs://bucket-b/path/*.parquet"
    val paths = getInputPaths(sc, path)
    paths should have length 2
    paths(0) shouldBe "gs://bucket-a/path/*.parquet"
    paths(1) shouldBe "gs://bucket-b/path/*.parquet"
  }

  it should "handle paths containing commas when escaped" in {
    val sc = ScioContext()
    // A single path with no commas should round-trip cleanly
    val path = "gs://bucket/no-commas/data/*.parquet"
    val paths = getInputPaths(sc, path)
    paths should have length 1
    paths(0) shouldBe path
  }
}
