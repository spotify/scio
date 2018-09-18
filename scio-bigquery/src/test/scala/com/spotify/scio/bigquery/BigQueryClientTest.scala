/*
 * Copyright 2018 Spotify AB.
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

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest._

class BigQueryClientTest
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {

  "BigQueryClient" should "throw an exception when an empty or null ProjectId is provided" in {
    assertThrows[IllegalArgumentException] {
      BigQueryClient("")
    }

    assertThrows[IllegalArgumentException] {
      BigQueryClient(null)
    }
  }

  it should "work with non-empty ProjectId" in {
    val projectIdGen = Gen.alphaNumStr.suchThat(_.nonEmpty)

    forAll(projectIdGen) { projectId =>
      BigQueryClient(projectId)
    }
  }

}
