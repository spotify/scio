/*
Copyright 2012 Twitter, Inc.

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

package com.spotify.scio.vendor.chill

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

trait AwesomeFns {
  val myfun: Int => Int = { x: Int => 2 * x }
}

object BaseFns extends AwesomeFns {
  val myfun2: Int => Int = { x: Int => 4 * x }
  def apply(x: Int): Int = myfun.apply(x)
}

trait AwesomeFn2 {
  def mult: Int
  val timesByMult: Int => Int = { x: Int => mult * x }
}

object BaseFns2 extends AwesomeFn2 {
  def mult = 5
}

class FunctionSerialization extends AnyWordSpec with Matchers with BaseProperties {
  "Serialize objects with Fns" should {
    "fn calling" in {
      // rt(fn).apply(4) should equal(8)
      // In the object:
      rt(BaseFns.myfun2).apply(4) should equal(16)

      // Inherited from the trait:
      rt(BaseFns.myfun).apply(4) should equal(8)
    }
    "roundtrip the object" in {
      rt(BaseFns) should equal(BaseFns)
    }
    "Handle traits with abstract vals/def" in {
      val bf2 = rt(BaseFns2)
      (bf2 eq BaseFns2) should equal(true)
      bf2 should equal(BaseFns2)
      bf2.timesByMult(10) should equal(50)
      val rtTBM = rt(BaseFns2.timesByMult)
      rtTBM.apply(10) should equal(50)
    }
  }
}
