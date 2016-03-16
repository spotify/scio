/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.scio.coders

import com.spotify.scio.avro.Account
import com.spotify.scio.testing._


class RichCoderRegistryTest extends PipelineSpec {

  "RichCoderRegistry" should "support Avro SpecificRecord in joins" in {
    val expected = Seq(
      new Account(1, "checking", "Alice", 1000.0),
      new Account(2, "checking", "Bob", 2000.0))

    runWithContext { sc =>
      val lhs = sc.parallelize(1 to 10).map(i => new Account(i, "checking", "user_" + i, i * 1000.0))
      val rhs = sc.parallelize(Seq(1 -> "Alice", 2 -> "Bob"))
      lhs
        .keyBy(_.getId.toInt)
        .join(rhs)
        .mapValues { case (a, n) => Account.newBuilder(a).setName(n).build() }
        .values should containInAnyOrder (expected)
    }
  }

}
