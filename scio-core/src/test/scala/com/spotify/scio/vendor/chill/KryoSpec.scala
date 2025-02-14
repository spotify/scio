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

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.immutable.{BitSet, HashMap, HashSet, ListMap, ListSet, SortedMap, SortedSet}
import scala.collection.mutable.{
  ArrayBuffer => MArrayBuffer,
  BitSet => MBitSet,
  HashMap => MHashMap
}
import _root_.java.util.PriorityQueue
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/*
 * This is just a test case for Kryo to deal with. It should
 * be outside KryoSpec, otherwise the enclosing class, KryoSpec
 * will also need to be serialized
 */
case class TestCaseClassForSerialization(x: String, y: Int)

case class TestValMap(map: Map[String, Double])
case class TestValHashMap(map: HashMap[String, Double])
case class TestVarArgs(vargs: String*)

class SomeRandom(val x: Int)

object WeekDay extends Enumeration {
  type WeekDay = Value
  val Mon, Tue, Wed, Thu, Fri, Sat, Sun = Value
}

trait ExampleUsingSelf { self =>
  def count = 0
  def addOne: ExampleUsingSelf = new ExampleUsingSelf { override def count: Int = self.count + 1 }
}

case class Foo(m1: Map[String, Int], m2: Map[String, Seq[String]])

class KryoSpec extends AnyWordSpec with Matchers with BaseProperties {
  def roundtrip[T]: Matcher[T] = new Matcher[T] {
    def apply(t: T): MatchResult =
      MatchResult(
        rtEquiv(t),
        "successful serialization roundtrip for " + t,
        "failed serialization roundtrip for " + t
      )
  }

  def getKryo: Kryo = KryoSerializer.registered.newKryo

  "KryoSerializers and KryoDeserializers" should {
    "round trip any non-array object" in {
      val test = List(
        1,
        2,
        "hey",
        (1, 2),
        ("hey", "you"),
        ("slightly", 1L, "longer", 42, "tuple"),
        Foo(Map("1" -> 1), Map("1" -> Seq("foo.com"))),
        Map(1 -> 2, 4 -> 5),
        0 to 100,
        (0 to 42).toList,
        Seq(1, 100, 1000),
        Right(Map("hello" -> 100)),
        Left(Map(1 -> "YO!")),
        Some(Left(10)),
        Map("good" -> 0.5, "bad" -> -1.0),
        Map('a -> 'a, 'b -> 'b, 'c -> 'c, 'd -> 'd, 'e -> 'e),
        MArrayBuffer(1, 2, 3, 4, 5),
        List(Some(MHashMap(1 -> 1, 2 -> 2)), None, Some(MHashMap(3 -> 4))),
        Set(1, 2, 3, 4, 10),
        HashSet(1, 2),
        SortedSet[Long](),
        SortedSet(1L, 2L, 3L, 4L),
        BitSet(),
        BitSet((0 until 1000).map { x: Int => x * x }: _*),
        MBitSet(),
        MBitSet((0 until 1000).map { x: Int => x * x }: _*),
        SortedMap[Long, String](),
        SortedMap("b" -> 2, "a" -> 1),
        ListMap("good" -> 0.5, "bad" -> -1.0),
        HashMap("good" -> 0.5, "bad" -> -1.0),
        TestCaseClassForSerialization("case classes are: ", 10),
        TestValMap(
          Map(
            "you" -> 1.0,
            "every" -> 2.0,
            "body" -> 3.0,
            "a" -> 1.0,
            "b" -> 2.0,
            "c" -> 3.0,
            "d" -> 4.0
          )
        ),
        TestValHashMap(HashMap("you" -> 1.0)),
        TestVarArgs("hey", "you", "guys"),
        implicitly[ClassTag[(Int, Int)]],
        Vector(1, 2, 3, 4, 5),
        TestValMap(null),
        Some("junk"),
        List(1, 2, 3).asJava,
        Map("hey" -> 1, "you" -> 2).asJava,
        new _root_.java.util.ArrayList(Seq(1, 2, 3).asJava).asScala,
        new _root_.java.util.HashMap[Int, Int](Map(1 -> 2, 3 -> 4).asJava).asScala,
        (),
        'hai,
        BigDecimal(1000.24)
      ).asInstanceOf[List[AnyRef]]

      test.foreach(_ should roundtrip)
    }
    "round trip a SortedSet" in {
      val a = SortedSet[Long]() // Test empty SortedSet
      val b = SortedSet[Int](1, 2) // Test small SortedSet
      val c =
        SortedSet[Int](1, 2, 3, 4, 6, 7, 8, 9, 10)(
          Ordering.fromLessThan((x, y) => x > y)
        ) // Test with different ordering
      a should roundtrip
      b should roundtrip
      c should roundtrip
      (rt(c) + 5) should equal(c + 5)
    }
    "round trip a ListSet" in {
      val a = ListSet[Long]() // Test empty SortedSet
      val b = ListSet[Int](1, 2) // Test small ListSet
      val c = ListSet[Int](1, 2, 3, 4, 6, 7, 8, 9, 10)
      a should roundtrip
      b should roundtrip
      c should roundtrip
    }
    "handle trait with reference of self" in {
      val a = new ExampleUsingSelf {}
      val b = rt(a.addOne)
      b.count should equal(1)
    }
    "handle manifests" in {
      manifest[Int] should roundtrip
      manifest[(Int, Int)] should roundtrip
      manifest[Array[Int]] should roundtrip
    }
    "handle arrays" in {
      def arrayRT[T](arr: Array[T]): Unit =
        // Array doesn't have a good equals
        rt(arr).toList should equal(arr.toList)
      arrayRT(Array(0))
      arrayRT(Array(0.1))
      arrayRT(Array("hey"))
      arrayRT(Array((0, 1)))
      arrayRT(Array((0, 1), (1, 0)))
      arrayRT(Array(None, Nil, None, Nil))
    }
    "handle WrappedArray instances" in {
      val tests = Seq(
        Array((1, 1), (2, 2), (3, 3)).toSeq,
        Array((1.0, 1.0), (2.0, 2.0)).toSeq,
        Array((1.0, "1.0"), (2.0, "2.0")).toSeq
      )
      tests.foreach(_ should roundtrip)
    }
    "handle lists of lists" in {
      List(("us", List(1)), ("jp", List(3, 2)), ("gb", List(3, 1))) should roundtrip
    }
    "handle scala singletons" in {
      List(Nil, None) should roundtrip
      None should roundtrip
      (rt(None) eq None) should equal(true)
    }
    "serialize a giant list" in {
      val bigList = (1 to 100000).toList
      val list2 = rt(bigList)
      list2.size should equal(bigList.size)
      // Specs, it turns out, also doesn't deal with giant lists well:
      list2.zip(bigList).foreach(tup => tup._1 should equal(tup._2))
    }
    "handle scala enums" in {
      WeekDay.values.foreach(_ should roundtrip)
    }
    "handle asJavaIterable" in {
      val col = Seq(12345).asJava
      col should roundtrip
    }
    "use java serialization" in {
      val kinst = { () => getKryo.javaForClass[TestCaseClassForSerialization] }
      rtEquiv(kinst, TestCaseClassForSerialization("hey", 42)) should equal(true)
    }
    "work with Meatlocker" in {
      val l = List(1, 2, 3)
      val ml = MeatLocker(l)
      jrt(ml).get should equal(l)
    }
    "work with Externalizer" in {
      val l = List(1, 2, 3)
      val ext = Externalizer(l)
      ext.javaWorks should equal(true)
      jrt(ext).get should equal(l)
    }
    "work with Externalizer with non-java-ser" in {
      val l = new SomeRandom(3)
      val ext = Externalizer(l)
      ext.javaWorks should equal(false)
      jrt(ext).get.x should equal(l.x)
    }
    "Externalizer can RT with Kryo" in {
      val l = new SomeRandom(10)
      val ext = Externalizer(l)
      rt(ext).get.x should equal(l.x)
    }
    "handle Regex" in {
      val test = """\bhilarious""".r
      val roundtripped = rt(test)
      roundtripped.pattern.pattern should equal(test.pattern.pattern)
      roundtripped.findFirstIn("hilarious").isDefined should equal(true)
    }
    "handle small immutable maps when registration is required" in {
      val inst = { () =>
        val kryo = KryoSerializer.registered.newKryo
        kryo.setRegistrationRequired(true)
        kryo
      }
      val m1 = Map('a -> 'a)
      val m2 = Map('a -> 'a, 'b -> 'b)
      val m3 = Map('a -> 'a, 'b -> 'b, 'c -> 'c)
      val m4 = Map('a -> 'a, 'b -> 'b, 'c -> 'c, 'd -> 'd)
      val m5 = Map('a -> 'a, 'b -> 'b, 'c -> 'c, 'd -> 'd, 'e -> 'e)
      Seq(m1, m2, m3, m4, m5).foreach(rtEquiv(inst, _) should equal(true))
    }
    "handle small immutable sets when registration is required" in {
      val inst = { () =>
        val kryo = getKryo
        kryo.setRegistrationRequired(true)
        kryo
      }
      val s1 = Set('a)
      val s2 = Set('a, 'b)
      val s3 = Set('a, 'b, 'c)
      val s4 = Set('a, 'b, 'c, 'd)
      val s5 = Set('a, 'b, 'c, 'd, 'e)
      Seq(s1, s2, s3, s4, s5).foreach(rtEquiv(inst, _) should equal(true))
    }
    "handle nested mutable maps" in {
      val inst = { () =>
        val kryo = getKryo
        kryo.setRegistrationRequired(true)
        kryo
      }
      val obj0 = mutable.Map(
        4 -> mutable.Set("house1", "house2"),
        1 -> mutable.Set("name3", "name4", "name1", "name2"),
        0 -> mutable.Set(1, 2, 3, 4)
      )

      // Make sure to make a totally separate map to check equality with
      mutable.Map(
        4 -> mutable.Set("house1", "house2"),
        1 -> mutable.Set("name3", "name4", "name1", "name2"),
        0 -> mutable.Set(1, 2, 3, 4)
      )

      rtEquiv(inst, obj0) should equal(true)
    }
    "deserialize InputStream" in {
      val obj = Seq(1, 2, 3)
      val bytes = serialize(obj)

      val inputStream = new _root_.java.io.ByteArrayInputStream(bytes)

      val kryo = getKryo
      val rich = new RichKryo(kryo)

      val opt1 = rich.fromInputStream(inputStream)
      opt1 should equal(Option(obj))

      // Test again to make sure it still works
      inputStream.reset()
      val opt2 = rich.fromInputStream(inputStream)
      opt2 should equal(Option(obj))
    }
    "deserialize ByteBuffer" in {
      val obj = Seq(1, 2, 3)
      val bytes = serialize(obj)

      val byteBuffer = _root_.java.nio.ByteBuffer.wrap(bytes)

      val kryo = getKryo
      val rich = new RichKryo(kryo)

      val opt1 = rich.fromByteBuffer(byteBuffer)
      opt1 should equal(Option(obj))

      // Test again to make sure it still works
      byteBuffer.rewind()
      val opt2 = rich.fromByteBuffer(byteBuffer)
      opt2 should equal(Option(obj))
    }
    "Handle Ordering.reverse" in {
      // This is exercising the synthetic field serialization in 2.10
      val ord = Ordering.fromLessThan[(Int, Int)]((l, r) => l._1 < r._1)
      // Now with a reverse ordering:
      val qr = new PriorityQueue[(Int, Int)](3, ord.reverse)
      qr.add((2, 3))
      qr.add((4, 5))
      def toList[A](q: PriorityQueue[A]): List[A] = {
        import scala.collection.JavaConverters._
        q.iterator.asScala.toList
      }
      val qrlist = toList(qr)
      toList(rt(qr)) should equal(qrlist)
    }
    "Ranges should be fixed size" in {
      val MAX_RANGE_SIZE = 453 // what seems to be needed.
      serialize(1 to 10000).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1 to 10000 by 2).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1 until 10000).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1 until 10000 by 2).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1L to 10000L).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1L to 10000L by 2L).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1L until 10000L).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(1L until 10000L by 2L).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(
        BigDecimal(1.0) to BigDecimal(10000.0)
      ).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(
        BigDecimal(1.0) to BigDecimal(10000.0) by 2.0
      ).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(
        BigDecimal(1.0) until BigDecimal(10000.0)
      ).size should be < MAX_RANGE_SIZE // some fixed size
      serialize(
        BigDecimal(1.0) until BigDecimal(10000.0) by 2.0
      ).size should be < MAX_RANGE_SIZE // some fixed size
    }
    "VolatileByteRef" in {
      import scala.runtime.VolatileByteRef

      val br0 = new VolatileByteRef(100: Byte)
      br0.elem = 42: Byte
      val br1 = rt(br0)
      assert(br0.elem == br1.elem)
    }
  }
}
