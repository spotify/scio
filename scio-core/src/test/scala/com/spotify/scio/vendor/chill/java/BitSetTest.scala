/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill.java

import java.util
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.util.Random
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BitSetSpec extends AnyWordSpec with Matchers {
  implicit val kryo: Kryo = new Kryo()

  def rt[A](a: A)(implicit k: Kryo): A = {
    val out = new Output(1000, -1)
    k.writeClassAndObject(out, a.asInstanceOf[AnyRef])
    val in = new Input(out.toBytes)
    k.readClassAndObject(in).asInstanceOf[A]
  }

  "A BitSetSerializer serializer" should {
    "handle BitSet" in {
      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
      BitSetSerializer.registrar()(kryo)
      var simple = new util.BitSet(2048)
      simple.size() must be(2048)
      for (i <- 0 to 1337) {
        simple.set(i, true)
      }
      // we assume everything after 1337 to be false
      simple.get(1338) must equal(false)
      simple.get(2000) must equal(false)
      val dolly = rt(simple)
      simple = null // avoid accidental calls
      dolly.size() must be(2048)
      for (i <- 0 to 1337) {
        dolly.get(i) must be(true)
      }
      dolly.get(1338) must equal(false)
      dolly.get(2000) must equal(false)
    }

    /**
     * My results: The old serializer took 2886ms The new serializer took 112ms The old serializer
     * needs 2051 bytes The new serializer needs 258 bytes
     */
    "handle a BitSet efficiently" in {
      val oldKryo = new Kryo()
      OldBitSetSerializer.registrar()(oldKryo)

      val newKryo = new Kryo()
      BitSetSerializer.registrar()(newKryo)

      val element = new util.BitSet(2048)
      val rnd = new Random()
      for (i <- 0 to 2048) {
        element.set(i, rnd.nextBoolean())
      }

      // warmup In case anybody wants to see hotspot
      var lastBitSetFromOld: util.BitSet = null
      for (_ <- 0 to 50000) {
        lastBitSetFromOld = rt(element)(oldKryo)
      }
      var start = System.currentTimeMillis()
      for (_ <- 0 to 100000) {
        rt(element)(oldKryo)
      }
      println("The old serializer took " + (System.currentTimeMillis() - start) + "ms")

      var lastBitSetFromNew: util.BitSet = null
      // warmup for the new kryo
      for (_ <- 0 to 50000) {
        lastBitSetFromNew = rt(element)(newKryo)
      }
      // check for the three bitsets to be equal
      for (i <- 0 to 2048) {
        // original bitset against old serializer output
        element.get(i) must be(lastBitSetFromOld.get(i))

        // original bitset against new serializer output
        element.get(i) must be(lastBitSetFromNew.get(i))
      }

      start = System.currentTimeMillis()
      for (_ <- 0 to 100000) {
        rt(element)(newKryo)
      }
      println("The new serializer took " + (System.currentTimeMillis() - start) + "ms")

      var out = new Output(1, -1)
      oldKryo.writeObject(out, element)
      out.flush()
      val oldBytes = out.total()
      println("The old serializer needs " + oldBytes + " bytes")
      out = new Output(1, -1)
      newKryo.writeObject(out, element)
      out.flush()
      val newBytes = out.total()
      println("The new serializer needs " + newBytes + " bytes")

      oldBytes >= newBytes must be(true)
    }
  }
}
