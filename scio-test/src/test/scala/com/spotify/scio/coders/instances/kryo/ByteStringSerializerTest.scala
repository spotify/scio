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
package com.spotify.scio.coders.instances.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.protobuf.ByteString
import com.twitter.chill.{Kryo, KryoSerializer}
import org.scalatest.{FlatSpec, Matchers}

class ByteStringSerializerTest extends FlatSpec with Matchers {

  private def testRoundTrip(ser: ByteStringSerializer, bs: ByteString): Unit = {
    val k: Kryo = KryoSerializer.registered.newKryo()
    val o = new Array[Byte](bs.size() * 2)
    ser.write(k, new Output(o), bs)
    val back = ser.read(k, new Input(o), null)
    bs shouldEqual back
    ()
  }

  "ByteStringSerializer" should "roundtrip large ByteString" in {
    val ser = new ByteStringSerializer
    testRoundTrip(ser, ByteString.copyFrom(Array.fill(1056)(7.toByte)))
  }
}
