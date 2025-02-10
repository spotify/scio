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

import java.io.ByteArrayOutputStream

import com.spotify.scio.vendor.chill.{KryoInstantiator, KryoPool}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SerDeStateTest extends AnyWordSpec with Matchers {
  "SerDeState" should {
    "Properly write out to a Stream" in {
      val pool = KryoPool.withBuffer(1, new KryoInstantiator, 1000, -1)
      val st = pool.borrow()

      st.writeClassAndObject("Hello World")
      val baos = new ByteArrayOutputStream
      st.writeOutputTo(baos)

      st.clear()
      st.setInput(baos.toByteArray)
      st.readClassAndObject() should equal("Hello World")
    }
  }
}
