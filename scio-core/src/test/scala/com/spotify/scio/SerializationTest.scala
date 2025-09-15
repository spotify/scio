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

package com.spotify.scio

import com.spotify.scio.values.SideOutput
import org.apache.beam.sdk.util.SerializableUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class SerializationTest extends AnyFlatSpec with Matchers {
  "Args" should "be serializable" in {
    SerializableUtils.ensureSerializable(Args(Array("--key=value")))
  }

  "DistCache" should "be serializable" in {
    val sc = ScioContext()
    val dc1 = sc.distCache("a.txt")(Source.fromFile(_).getLines())
    val dc2 =
      sc.distCache(Seq("a.txt", "b.txt"))(_.map(Source.fromFile(_).getLines()))
    SerializableUtils.ensureSerializable(dc1)
    SerializableUtils.ensureSerializable(dc2)
  }

  "SideInput" should "be serializable" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(1 to 10)
    SerializableUtils.ensureSerializable(p1.asSingletonSideInput)
    SerializableUtils.ensureSerializable(p1.asListSideInput)
    SerializableUtils.ensureSerializable(p1.asIterableSideInput)
    SerializableUtils.ensureSerializable(p1.asSetSingletonSideInput)
    val p2 = sc.parallelize(1 to 10).map(i => (s"k$i", s"v$i"))
    SerializableUtils.ensureSerializable(p2.asMapSideInput)
    SerializableUtils.ensureSerializable(p2.asMultiMapSideInput)
    SerializableUtils.ensureSerializable(p2.asMapSingletonSideInput)
    SerializableUtils.ensureSerializable(p2.asMultiMapSingletonSideInput)
  }

  "SideOutput" should "be serializable" in {
    SerializableUtils.ensureSerializable(SideOutput[Int]())
  }
}
