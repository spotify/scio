/*
 * Copyright 2016 Alex Chermenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.spotify.scio.vendor.chill.java

import TestLists._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ArraysAsListTest extends AnyWordSpec with Matchers {
  "ArraysAsListSerializer" should {
    "byte[]" in {
      val list = getList(java.lang.Byte.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "short[]" in {
      val list = getList(java.lang.Short.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "int[]" in {
      val list = getList(java.lang.Integer.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "float[]" in {
      val list = getList(java.lang.Float.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "double[]" in {
      val list = getList(java.lang.Double.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "char[]" in {
      val list = getList(java.lang.Character.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "boolean[]" in {
      val list = getList(java.lang.Boolean.TYPE)
      list should equal(serializeAndDeserialize(list))
    }

    "String[]" in {
      val list = getList(classOf[String])
      list should equal(serializeAndDeserialize(list))
    }

    "Object[][]" in {
      val list = getList(classOf[java.util.List[_]])
      list should equal(serializeAndDeserialize(list))
    }
  }
}
