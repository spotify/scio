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

import com.spotify.scio.vendor.chill.ClosureCleaner.{deserialize, serialize}
import org.scalatest._
import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks._

import scala.reflect.ClassTag
import scala.util.Try
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

object ClosureCleanerSpec {
  // scala 2.12.x has better Java 8 interop. Lambdas are serializable and implemented via
  // invokeDynamic and the use of LambdaMetaFactory.
  val supportsSerializedLambda: Boolean =
    List("2.12", "2.13").exists(scala.util.Properties.versionString.contains)
}

class ClosureCleanerSpec extends AnyWordSpec with Matchers {
  private val someSerializableValue = 1
  private def someSerializableMethod() = 1

  "ClosureCleaner" should {
    "clean normal objects" in {
      val myList = List(1, 2, 3)
      serializable(myList, before = true, after = true)

      case class Test(x: Int)
      val t = Test(3)
      serializable(t, before = false, after = true)
    }

    "clean basic serializable closures" in {
      val localVal = 1
      val closure1 = (_: Int) => localVal
      val closure2 = (x: Int) => x + localVal

      serializableFn(closure1, before = true, after = true)
      serializableFn(closure2, before = true, after = true)
    }

    "clean basic closures (non LMF)" in {
      assume(!ClosureCleanerSpec.supportsSerializedLambda)

      val closure1 = (_: Int) => someSerializableValue
      val closure2 = (_: Int) => someSerializableMethod()

      serializableFn(closure1, before = false, after = true)
      serializableFn(closure2, before = false, after = true)
    }

    "clean basic closures (LMF)" in {
      assume(ClosureCleanerSpec.supportsSerializedLambda)

      val closure1 = (_: Int) => someSerializableValue
      val closure2 = (_: Int) => someSerializableMethod()

      serializableFn(closure1, before = false, after = false)
      serializableFn(closure2, before = false, after = false)
    }

    "clean functions in traits" in {
      // not serializable; AwesomeFn2 needs to extend Serializable
      val fn = BaseFns2.timesByMult
      serializableFn(fn, before = false, after = false)
    }

    "clean basic nested closures (non LMF)" in {
      assume(!ClosureCleanerSpec.supportsSerializedLambda)

      val closure1 = (i: Int) => {
        Option(i).map(x => x + someSerializableValue)
      }
      val closure2 = (j: Int) => {
        Option(j).map(x => x + someSerializableMethod())
      }
      val closure3 = (m: Int) => {
        Option(m).foreach(x =>
          Option(x).foreach(y => Option(y).foreach(_ => someSerializableValue))
        )
      }

      serializableFn(closure1, before = false, after = true)
      serializableFn(closure2, before = false, after = true)
      serializableFn(closure3, before = false, after = true)
    }

    "clean basic nested closures (LMF)" in {
      assume(ClosureCleanerSpec.supportsSerializedLambda)

      val closure1 = (i: Int) => {
        Option(i).map(x => x + someSerializableValue)
      }
      val closure2 = (j: Int) => {
        Option(j).map(x => x + someSerializableMethod())
      }
      val closure3 = (m: Int) => {
        Option(m).foreach(x =>
          Option(x).foreach(y => Option(y).foreach(_ => someSerializableValue))
        )
      }

      serializableFn(closure1, before = false, after = false)
      serializableFn(closure2, before = false, after = false)
      serializableFn(closure3, before = false, after = false)
    }

    "clean outers with constructors" in {
      class Test(x: String) {
        val l: Int = x.length
        def rev(y: String): Int = (x + y).length
      }

      val t = new Test("you all everybody")
      val fn: String => Int = t.rev
      serializableFn(fn, before = false, after = false)
    }

    "clean outers with constructors only non-LMF" in {
      assume(!ClosureCleanerSpec.supportsSerializedLambda)
      // currently this doesn't work with 2.12.x; SerializedLambda does not provide an outer ref.
      // need to find another way...
      serializableFn(new TestClass().fn, before = false, after = true)
    }

    "clean outers with objects" in {
      serializableFn(TestObject.fn, before = true, after = true)
    }

    "clean complex nested closures (non LMF)" in {
      assume(!ClosureCleanerSpec.supportsSerializedLambda)

      serializableFn(new NestedClosuresNotSerializable().getMapFn, before = false, after = true)

      class A(val f: Int => Int)
      class B(val f: Int => Int)
      class C extends A(x => x * x)
      class D extends B(x => new C().f(x))

      serializableFn(new D().f, before = false, after = true)
    }

    "clean complex nested closures (LMF)" in {
      assume(ClosureCleanerSpec.supportsSerializedLambda)

      serializableFn(new NestedClosuresNotSerializable().getMapFn, before = true, after = true)

      class A(val f: Int => Int)
      class B(val f: Int => Int)
      class C extends A(x => x * x)
      class D extends B(x => new C().f(x))

      serializableFn(new D().f, before = false, after = true)
    }
  }

  private def isSerializable[T](obj: T): Boolean =
    Try(deserialize[T](serialize(obj))).isSuccess

  @nowarn private def serializable[A <: AnyRef: ClassTag](
    fn: A,
    before: Boolean,
    after: Boolean
  ): Assertion = {
    val fn0 = fn
    assert(isSerializable(fn0) == before, "serializable before")
    val clean = ClosureCleaner.clean(fn)
    assert(isSerializable(clean) == after, "serializable after")
    assert(clean == fn0)
  }

  private def serializableFn[A: Arbitrary, B](
    fn: => A => B,
    before: Boolean,
    after: Boolean
  ): Assertion = {
    val fn0 = fn
    assert(isSerializable(fn0) == before, "serializable before")
    val clean = ClosureCleaner.clean(fn)
    assert(isSerializable(clean) == after, "serializable after")
    forAll { a: A => assert(clean(a) == fn0(a)) }
  }
}

class NestedClosuresNotSerializable {
  val irrelevantInt: Int = 1
  def closure(@nowarn name: String)(body: => Int => Int): Int => Int = body
  def getMapFn: Int => Int = closure("one") {
    @nowarn def x = irrelevantInt // scalafix:ok
    def y = 2
    val fn = { a: Int => a + y }
    fn
  }
}

trait NotSerializable

object TestObject {
  val bar: () => Int = () => 1
  // this is not actually used by any closure and is not serializable
  val boom: NotSerializable = new NotSerializable {}

  // we really need outer because we access this.bar
  val fn: Int => Int = { x: Int => bar() + x }
}

class TestClass extends Serializable {
  val bar: Int = 1
  // this is not actually used by any closure and is not serializable
  var boom: NotSerializable = new NotSerializable {}

  // we really need outer because we access this.bar
  val fn: Int => Int = (x: Int) => bar + x
}
