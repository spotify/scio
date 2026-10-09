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

package com.spotify.scio.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CacheTest extends AnyFlatSpec with Matchers {
  "Cache.guava" should "return null when the default value is null" in {
    val cache = Cache.guava[String, String]()
    cache.get("missing", null.asInstanceOf[String]) shouldBe null
  }

  it should "cache non-null default values" in {
    val cache = Cache.guava[String, String]()
    cache.get("k", "v") shouldBe "v"
    cache.get("k") shouldBe Some("v")
  }

  "Cache.caffeine" should "return null when the default value is null" in {
    val cache = Cache.caffeine[String, String]
    cache.get("missing", null.asInstanceOf[String]) shouldBe null
  }
}
