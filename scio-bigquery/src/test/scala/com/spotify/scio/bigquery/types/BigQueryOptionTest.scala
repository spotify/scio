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

package com.spotify.scio.bigquery.types

import org.scalatest.{FlatSpec, Matchers}

object BigQueryOptionTest {

  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A1

  @BigQueryOption.flattenResults
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A2

  @BigQueryOption.flattenResults()
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A3

  @BigQueryOption.flattenResults(false)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A4

  @BigQueryOption.flattenResults(true)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A5

  @BigQueryOption.flattenResults(value = false)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A6

  @BigQueryOption.flattenResults(value = true)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A7

}

class BigQueryOptionTest extends FlatSpec with Matchers {

  import BigQueryOptionTest._

  "BigQueryOption" should "have default flattenResults" in {
    BigQueryType[A1].flattenResults shouldBe false
  }

  it should "support flattenResults" in {
    BigQueryType[A2].flattenResults shouldBe true
  }

  it should "support flattenResults()" in {
    BigQueryType[A3].flattenResults shouldBe true
  }

  it should "support flattenResults(false)" in {
    BigQueryType[A4].flattenResults shouldBe false
  }

  it should "support flattenResults(true)" in {
    BigQueryType[A5].flattenResults shouldBe true
  }

  it should "support flattenResults(value = false)" in {
    BigQueryType[A6].flattenResults shouldBe false
  }

  it should "support flattenResults(value = true)" in {
    BigQueryType[A7].flattenResults shouldBe true
  }

}
