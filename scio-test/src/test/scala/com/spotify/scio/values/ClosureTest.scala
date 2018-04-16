/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class ClosureTest extends PipelineSpec {

  "SCollection" should "support lambda" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(_ * 10) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support def fn()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      def fn(x: Int): Int = x * 10
      p.map(fn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support val fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val fn = (x: Int) => x * 10
      p.map(fn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  def classFn(x: Int): Int = x * 10

  it should "support class fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(classFn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support object fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(ClosureTest.objectFn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support lambda with val from closure" in {
    runWithContext { sc =>
      val x = 10
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(_ * x) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support lambda with param from closure" in {
    runWithContext { sc =>
      def bar(in: SCollection[Int], x: Int): SCollection[Int] = in.map(_ * x)
      val p = sc.parallelize(Seq(1, 2, 3))
      bar(p, 10) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support lambda with object member from closure" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      Foo.bar(p) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support lambda with class member from closure" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val r = new Foo().bar(p)
      r should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "print readable error message for unserializable fn" in {
    val thrown = the[IllegalArgumentException] thrownBy
      runWithContext { sc =>
        val o = new NotSerializableObj()
        sc.parallelize(Seq(1, 2, 3))
          .map(_ * o.x)
      }
    val fnString = "map@{ClosureTest.scala:"
    thrown.getMessage should include (fnString)
  }

  it should "support multi-nested closures" in {
    runWithContext { sc =>
      val fn = new NestedClosuresNotSerializable().getMapFn
      val p = sc.parallelize(Seq(1, 2, 3))
        .map(fn)
      p should containInAnyOrder (Seq(3, 4, 5))
    }
  }
}

object ClosureTest {
  def objectFn(x: Int): Int = x * 10
}

object Foo extends Serializable {
  val x = 10
  def bar(in: SCollection[Int]): SCollection[Int] = in.map(_ * x)
}

class Foo extends Serializable {
  val x = 10
  def bar(in: SCollection[Int]): SCollection[Int] = in.map(_ * x)
}

class NotSerializableObj {
  val x = 10
}

class NestedClosuresNotSerializable {
  val irrelevantInt: Int = 1
  def closure(name: String)(body: => Int => Int): Int => Int = body
  def getMapFn: Int => Int = closure("one") {
    def x = irrelevantInt
    def y = 2
    val fn = { a: Int => (a + y) }
    fn
  }
}
