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

package com.spotify.scio.vendor.chill.java

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import org.objenesis.strategy.StdInstantiatorStrategy
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PriorityQueueSpec extends AnyWordSpec with Matchers {
  def rt[A](k: Kryo, a: A): A = {
    val out = new Output(1000, -1)
    k.writeClassAndObject(out, a.asInstanceOf[AnyRef])
    val in = new Input(out.toBytes)
    k.readClassAndObject(in).asInstanceOf[A]
  }

  "A PriorityQueue Serializer" should {
    "handle PriorityQueue" in {
      import scala.collection.JavaConverters._

      val kryo = new Kryo()
      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
      PriorityQueueSerializer.registrar()(kryo)
      new Java8ClosureRegistrar()(kryo)
      val ord = Ordering.fromLessThan[(Int, Int)]((l, r) => l._1 < r._1)
      val q = new java.util.PriorityQueue[(Int, Int)](3, ord)
      q.add((2, 3))
      q.add((4, 5))
      def toList[A](q: java.util.PriorityQueue[A]): List[A] =
        q.iterator.asScala.toList
      val qlist = toList(q)
      val newQ = rt(kryo, q)
      toList(newQ) should equal(qlist)
      newQ.add((1, 1))
      newQ.add((2, 1)) should equal(true)
      // Now without an ordering:
      val qi = new java.util.PriorityQueue[Int](3)
      qi.add(2)
      qi.add(5)
      val qilist = toList(qi)
      toList(rt(kryo, qi)) should equal(qilist)
      // Now with a reverse ordering
      // Note that in chill-scala, synthetic fields are not ignored by default
      // using the ScalaKryoInstantiator
      val synthF =
        new com.esotericsoftware.kryo.serializers.FieldSerializer(kryo, ord.reverse.getClass)
      synthF.setIgnoreSyntheticFields(false)
      kryo.register(ord.reverse.getClass, synthF)
      val qr = new java.util.PriorityQueue[(Int, Int)](3, ord.reverse)
      qr.add((2, 3))
      qr.add((4, 5))
      val qrlist = toList(qr)
      toList(rt(kryo, qr)) should equal(qrlist)
    }
  }
}
