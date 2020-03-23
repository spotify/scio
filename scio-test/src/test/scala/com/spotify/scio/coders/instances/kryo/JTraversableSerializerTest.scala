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
import com.twitter.chill.{Kryo, KryoSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

class JTraversableSerializerTest extends AnyFlatSpec with Matchers {
  private def testRoundTrip[T: ClassTag, C <: Iterable[T]](
    ser: JTraversableSerializer[T, C],
    elems: C
  ): Unit = {
    val k: Kryo = KryoSerializer.registered.newKryo()
    val bufferSize: Int = 1024
    val o = new Array[Byte](bufferSize)
    ser.write(k, new Output(o), elems)
    val back = ser.read(k, new Input(o), null)
    elems.size shouldBe back.size
    (elems: Iterable[T]) should contain theSameElementsAs back
    ()
  }

  "JIterableWrapperSerializer" should "cope with the internal buffer overflow" in {
    val ser = new JTraversableSerializer[String, Seq[String]](128)
    val input = List.fill(100)("foo")
    testRoundTrip(ser, input)
  }

  it should "be able to serialize a object larger than capacity of the internal buffer" in {
    val ser = new JTraversableSerializer[String, Seq[String]](1)
    val input = Seq("o" * 3)
    testRoundTrip(ser, input)
  }
}
