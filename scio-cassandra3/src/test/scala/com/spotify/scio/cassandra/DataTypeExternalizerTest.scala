/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.cassandra

import com.datastax.driver.core.DataType
import org.apache.beam.sdk.util.SerializableUtils
import org.scalatest._

class DataTypeExternalizerTest extends FlatSpec with Matchers {

  "DataTypeExternalizer" should "support ImmutableList" in {
    val dt = DataType.list(DataType.text())
    SerializableUtils
      .ensureSerializable(DataTypeExternalizer(dt))
      .get shouldBe dt
  }

  it should "support ImmutableSet" in {
    val dt = DataType.set(DataType.text())
    SerializableUtils
      .ensureSerializable(DataTypeExternalizer(dt))
      .get shouldBe dt
  }

}
