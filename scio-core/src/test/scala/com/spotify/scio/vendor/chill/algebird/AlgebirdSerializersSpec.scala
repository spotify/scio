/*
Copyright 2014 Twitter, Inc.

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

package com.spotify.scio.vendor.chill.algebird

import com.spotify.scio.vendor.chill.{KryoPool, ScalaKryoInstantiator}
import com.twitter.algebird.{
  AdaptiveVector,
  AveragedValue,
  DecayedValue,
  HyperLogLogMonoid,
  MomentsGroup,
  QTree
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AlgebirdSerializersSpec extends AnyWordSpec with Matchers {
  val kryo: KryoPool = {
    val inst = () => {
      val newK = (new ScalaKryoInstantiator).newKryo
      newK.setReferences(false) // typical in production environment (scalding, spark)
      (new AlgebirdRegistrar).apply(newK)
      newK
    }
    KryoPool.withByteArrayOutputStream(1, inst)
  }

  def roundtrip[X](x: X): Unit = {
    val bytes = kryo.toBytesWithClass(x)
    // println("bytes size : " + bytes.size)
    // println("bytes: " + new String(bytes, "UTF-8"))
    val result = kryo.fromBytes(bytes).asInstanceOf[X]
    result should equal(x)
  }

  def roundtripNoEq[X](x: X)(f: X => Any): Unit = {
    val bytes = kryo.toBytesWithClass(x)
    val result = kryo.fromBytes(bytes).asInstanceOf[X]
    f(result) should equal(f(x))
  }

  "kryo with AlgebirdRegistrar" should {
    "serialize and deserialize AveragedValue" in {
      roundtrip(AveragedValue(10L, 123.45))
    }

    "serialize and deserialize DecayedValue" in {
      roundtrip(DecayedValue.build(3.14, 20.2, 9.33))
    }

    "serialize and deserialize HyperLogLogMonoid" in {
      roundtripNoEq(new HyperLogLogMonoid(12))(_.bits)
    }

    "serialize and deserialize Moments" in {
      roundtrip(MomentsGroup.zero)
    }

    "serialize and deserialize QTree" in {
      roundtrip(QTree(1.0))
    }

    "serialize and deserialize HLL" in {
      val sparse = new HyperLogLogMonoid(4).create(Array(-127.toByte))
      val dense =
        new HyperLogLogMonoid(4).batchCreate(
          Seq(-127, 100, 23, 44, 15, 96, 10).map(x => Array(x.toByte))
        )
      roundtrip(sparse)
      roundtrip(dense)
    }

    "serialize and deserialize SparseVector and DenseVector" in {
      val sparse = AdaptiveVector.fromVector(Vector(1, 1, 1, 1, 1, 3), 1)
      val dense = AdaptiveVector.fromVector(Vector(1, 2, 3, 1, 2, 3), 1)
      roundtrip(sparse)
      roundtrip(dense)
    }
  }
}
