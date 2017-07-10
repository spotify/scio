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
package com.spotify.scio.coders

import com.esotericsoftware.kryo.io.{Input, Output}
import com.spotify.scio.util.ScioUtil
import com.twitter.chill.{Kryo, KryoSerializer}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

class JIterableWrapperSerializerTest extends FlatSpec with Matchers {

  private def testRoundTrip[T : ClassTag](ser: JIterableWrapperSerializer[T],
                                  elems: Iterable[T],
                                  k: Kryo = KryoSerializer.registered.newKryo(),
                                  bufferSize: Int = 1024): Unit = {
    val o = new Array[Byte](bufferSize)
    ser.write(k, new Output(o), elems)
    elems should contain theSameElementsAs ser.read(k, new Input(o), ScioUtil.classOf[Iterable[T]])
  }

  "JIterableWrapperSerializer" should "cope with the internal buffer overflow" in {
    val ser = new JIterableWrapperSerializer[String](128, 128)
    val input = List.fill(100)("foo")
    testRoundTrip(ser, input)
  }

  it should "be able to serialize a object larger than initial capacity of the internal buffer" in {
    val ser = new JIterableWrapperSerializer[String](1)
    val input = Seq("o" * 3)
    testRoundTrip(ser, input)
  }

  it should "be able to serialize a object larger than max capacity of the internal buffer" in {
    val ser = new JIterableWrapperSerializer[String](5, 10)
    val input = Seq("o" * 10)
    testRoundTrip(ser, input)
  }

}
