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

package com.spotify.scio.io

import com.spotify.scio.ScioContext
import org.scalatest.{FlatSpec, Matchers}

class InMemorySinkTest extends FlatSpec with Matchers {
  "InMemoryTap" should "return containing items as iterable" in {
    val sc = ScioContext.forTest()
    val items = sc.parallelize(List("String1", "String2"))
    val tap = TapOf[String].saveForTest(items)
    sc.run().waitUntilDone()
    tap.value.toList should contain allOf ("String1", "String2")
  }

  it should "return empty iterable when SCollection is empty" in {
    val sc = ScioContext.forTest()
    val items = sc.parallelize[String](List())
    val tap = TapOf[String].saveForTest(items)
    sc.run().waitUntilDone()
    tap.value shouldBe empty
  }
}
