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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class CoreSiteXmlTest extends AnyFlatSpec with Matchers {

  "core-site.xml" should "be identical in scio-parquet and scio-smb" in {
    val parquetCoreSite = Paths.get("src/main/resources/core-site.xml")
    val smbCoreSite = Paths.get("../scio-smb/src/main/resources/core-site.xml")

    Files.exists(parquetCoreSite) shouldBe true
    Files.exists(smbCoreSite) shouldBe true

    val parquetContent = Files.readString(parquetCoreSite)
    val smbContent = Files.readString(smbCoreSite)

    parquetContent shouldBe smbContent
  }
}
