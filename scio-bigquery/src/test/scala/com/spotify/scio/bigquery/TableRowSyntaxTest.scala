/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class TableRowSyntaxTest extends AnyFlatSpec with Matchers {
  "TableRowOps" should "#2673: not throw on get record" in {
    val json = """{"record":{"foo":"bar"}}"""
    val row = com.spotify.scio.util.ScioUtil.jsonFactory.fromString(json, classOf[TableRow])
    val expected = Map("foo" -> "bar")
    row.getRecord("record") shouldBe expected
  }
}
