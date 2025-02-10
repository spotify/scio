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

package com.spotify.scio.vendor.chill

import scala.collection.immutable.{
  HashMap,
  HashSet,
  ListMap,
  ListSet,
  Queue,
  TreeMap,
  TreeSet,
  WrappedString
}
import scala.collection.mutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StandardDataRegistrationsSpec extends AnyWordSpec with Matchers {
  s"""
    |For projects using chill to persist serialized data (for example in event
    |sourcing scenarios), it can be beneficial or even required to turn on the
    |Kryo.setRegistrationRequired setting. For such projects, chill should provide
    |registrations for the most common data structures that are likely to be
    |persisted.
    |
    |Note that for sorted sets and maps, only the natural orderings for Byte, Short,
    |Int, Long, Float, Double, Boolean, Char, and String are registered (and not for
    |example the reverse orderings).
    |
    |In addition to what is ensured by ${classOf[KryoSpec].getSimpleName},
    |the ScalaKryoInstantiator with setRegistrationRequired(true)""".stripMargin
    .should {
      def registrationRequiredInstantiator = new ScalaKryoInstantiator() {
        override def newKryo: KryoBase = {
          val k = super.newKryo
          k.setRegistrationRequired(true)
          k
        }
      }
      val kryo = KryoPool.withByteArrayOutputStream(4, registrationRequiredInstantiator)
      def roundtrip(original: AnyRef): Unit =
        try {
          val serde = kryo.fromBytes(kryo.toBytesWithClass(original))
          (original, serde) match {
            case (originalArray: Array[_], serdeArray: Array[_]) =>
              assert(originalArray.toSeq == serdeArray.toSeq)
            case _ =>
              assert(serde == original)
          }
        } catch {
          case e: Throwable =>
            val message =
              s"exception during serialization round trip for $original of ${original.getClass}:\n" +
                e.toString.linesIterator.next
            assert(false, message)
        }
      def tuples(count: Int): Seq[(Int, Int)] = Seq.range(0, count).map(n => (n, n + 1))
      "serialize the empty map" in { roundtrip(Map()) }
      "serialize the one-element map" in { roundtrip(Map(1 -> 2)) }
      "serialize a filtered map" in { roundtrip(Map(1 -> 2).filterKeys(_ != 2).toMap) }
      "serialize a mapped values map" in { roundtrip(Map(1 -> 2).mapValues(_ + 1).toMap) }
      "serialize larger maps" in {
        roundtrip(Map(tuples(2): _*), Map(tuples(3): _*), Map(tuples(4): _*), Map(tuples(5): _*))
      }
      "serialize the empty hash map" in { roundtrip(HashMap()) }
      "serialize the one-element hash map" in { roundtrip(HashMap(1 -> 2)) }
      "serialize larger hash maps" in {
        roundtrip(
          HashMap(tuples(2): _*),
          HashMap(tuples(3): _*),
          HashMap(tuples(4): _*),
          HashMap(tuples(5): _*)
        )
      }
      "serialize the empty list map" in { roundtrip(ListMap()) }
      "serialize the one-element list map" in { roundtrip(ListMap(1 -> 2)) }
      "serialize larger list maps" in {
        roundtrip(
          ListMap(tuples(2): _*),
          ListMap(tuples(3): _*),
          ListMap(tuples(4): _*),
          ListMap(tuples(5): _*)
        )
      }
      "serialize the empty tree map" in { roundtrip(TreeMap.empty[Int, Int]) }
      "serialize the one-element tree map" in { roundtrip(TreeMap(1 -> 2)) }
      "serialize larger tree maps" in {
        roundtrip(
          TreeMap(tuples(2): _*),
          TreeMap(tuples(3): _*),
          TreeMap(tuples(4): _*),
          TreeMap(tuples(5): _*)
        )
      }
      "serialize the empty set" in { roundtrip(Set()) }
      "serialize larger sets" in {
        roundtrip(Set(1), Set(1, 2), Set(1, 2, 3), Set(1, 2, 3, 4), Set(1, 2, 3, 4, 5))
      }
      "serialize the empty hash set" in { roundtrip(HashSet()) }
      "serialize the one-element hash set" in { roundtrip(HashSet(1)) }
      "serialize larger hash sets" in {
        roundtrip(HashSet(1, 2), HashSet(1, 2, 3), HashSet(1, 2, 3, 4), HashSet(1, 2, 3, 4, 5))
      }
      "serialize the empty list set" in { roundtrip(ListSet()) }
      "serialize the one-element list set" in { roundtrip(ListSet(1)) }
      "serialize larger list sets" in {
        roundtrip(ListSet(1, 2), ListSet(1, 2, 3), ListSet(1, 2, 3, 4), ListSet(1, 2, 3, 4, 5))
      }
      "serialize the empty tree set" in { roundtrip(TreeSet.empty[Int]) }
      "serialize the one-element tree set" in { roundtrip(TreeSet(1)) }
      "serialize larger tree sets" in {
        roundtrip(TreeSet(1, 2), TreeSet(1, 2, 3), TreeSet(1, 2, 3, 4), TreeSet(1, 2, 3, 4, 5))
      }
      "serialize a map's key set" in { roundtrip(Map(1 -> 2).keySet) }
      "serialize the empty list" in { roundtrip(Nil) }
      "serialize the one-element list" in { roundtrip(List(1)) }
      "serialize alternative ways to instantiate lists" in { roundtrip(List.empty[Int], 1 :: Nil) }
      "serialize larger lists" in {
        roundtrip(List(1, 2), List(1, 2, 3), List(1, 2, 3, 4), List(1, 2, 3, 4, 5))
      }
      "serialize the empty queue" in { roundtrip(Queue.empty[Int]) }
      "serialize the no-elements queue" in { roundtrip(Queue()) }
      "serialize larger queues" in {
        roundtrip(Queue(1), Queue(1, 2), Queue(1, 2, 3), Queue(1, 2, 3, 4), Queue(1, 2, 3, 4, 5))
      }
      "serialize a range" in { roundtrip(Range(2, 10, 3)) }
      "serialize vectors" in {
        roundtrip(
          Vector(),
          Vector(1),
          Vector(1, 2),
          Vector(1, 2, 3),
          Vector(1, 2, 3, 4),
          Vector(1, 2, 3, 4, 5)
        )
      }
      "serialize the empty stream" in { roundtrip(Stream()) }
      "serialize the one-element stream" in { roundtrip(Stream(1)) }
      "serialize larger streams" in {
        roundtrip(Stream(1, 2), Stream(1, 2, 3), Stream(1, 2, 3, 4), Stream(1, 2, 3, 4, 5))
      }
      "serialize the options" in { roundtrip(None, Some(1), Option.empty[Int], Option(3)) }
      "serialize the eithers" in { roundtrip(Left(2), Right(4), Left.apply[Int, Int](3)) }
      "serialize the empty array" in { roundtrip(Array()) }
      "serialize empty Int arrays" in { roundtrip(Array.empty[Int]) }
      "serialize Int arrays" in { roundtrip(Array(4, 2)) }
      "serialize empty Short arrays" in { roundtrip(Array.empty[Short]) }
      "serialize Short arrays" in { roundtrip(Array(3.toShort, 4.toShort)) }
      "serialize empty Byte arrays" in { roundtrip(Array.empty[Byte]) }
      "serialize Byte arrays" in { roundtrip(Array(3.toByte, 4.toByte)) }
      "serialize empty Long arrays" in { roundtrip(Array.empty[Long]) }
      "serialize Long arrays" in { roundtrip(Array(3L, 5L)) }
      "serialize empty Float arrays" in { roundtrip(Array.empty[Float]) }
      "serialize Float arrays" in { roundtrip(Array(3f, 5.3f)) }
      "serialize empty Double arrays" in { roundtrip(Array.empty[Double]) }
      "serialize Double arrays" in { roundtrip(Array(4d, 3.2d)) }
      "serialize empty Boolean arrays" in { roundtrip(Array.empty[Boolean]) }
      "serialize Boolean arrays" in { roundtrip(Array(true, false)) }
      "serialize empty Char arrays" in { roundtrip(Array.empty[Char]) }
      "serialize Char arrays" in { roundtrip(Array('a', 'b')) }
      "serialize empty String arrays" in { roundtrip(Array.empty[String]) }
      "serialize String arrays" in { roundtrip(Array("a", "")) }
      "serialize empty Object arrays" in { roundtrip(Array.empty[Object]) }
      "serialize Object arrays" in { roundtrip(Array("a", List())) }
      "serialize empty Any arrays" in { roundtrip(Array.empty[Any]) }
      "serialize Any arrays" in { roundtrip(Array("a", 3, Nil)) }
      "serialize the empty wrapped array" in { roundtrip(mutable.WrappedArray.empty[Object]) }
      "serialize empty Int wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Byte]()))
      }
      "serialize Int wrapped arrays" in { roundtrip(mutable.WrappedArray.make(Array[Byte](1, 3))) }
      "serialize empty Byte wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Short]()))
      }
      "serialize empty Short wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Int]()))
      }
      "serialize empty Long wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Long]()))
      }
      "serialize empty Float wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Float]()))
      }
      "serialize empty Double wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Double]()))
      }
      "serialize empty Boolean wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Boolean]()))
      }
      "serialize empty Char wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[Char]()))
      }
      "serialize empty String wrapped arrays" in {
        roundtrip(mutable.WrappedArray.make(Array[String]()))
      }
      "serialize wrapped strings" in { roundtrip(new WrappedString("abc")) }
    }
}
