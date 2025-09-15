/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.coders

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.protobuf.ByteString
import com.spotify.scio.proto.OuterClassForProto
import com.spotify.scio.testing.CoderAssertions._
import com.spotify.scio.coders.instances._
import com.spotify.scio.options.ScioOptions
import com.test.ZstdTestCaseClass
import com.twitter.algebird.Moments
import org.apache.beam.sdk.{coders => beam}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{BigEndianLongCoder, ZstdCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.commons.collections.IteratorUtils
import org.apache.commons.io.output.NullOutputStream
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.scalaccompat.annotation.nowarn

import java.io.{ByteArrayInputStream, ObjectOutputStream, ObjectStreamClass}
import java.nio.charset.Charset
import java.time._
import java.util.UUID
import scala.collection.{mutable => mut}
import scala.collection.compat._
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

final class CoderTest extends AnyFlatSpec with Matchers {
  import com.spotify.scio.coders.CoderTestUtils._

  val userId: UserId = UserId(Seq[Byte](1, 2, 3, 4))
  val user: User = User(userId, "johndoe", "johndoe@spotify.com")

  def materialize[T](coder: Coder[T]): beam.Coder[T] =
    CoderMaterializer.beam(PipelineOptionsFactory.create(), coder)

  it should "support primitives" in {
    false coderShould roundtripToBytes(Array(0)) and
      beOfType[Beam[_]] and
      materializeTo[beam.BooleanCoder]

    1 coderShould roundtripToBytes(Array(1)) and
      beOfType[Beam[_]] and
      materializeTo[beam.VarIntCoder]

    1L coderShould roundtripToBytes(Array(0, 0, 0, 0, 0, 0, 0, 1)) and
      beOfType[Beam[_]] and
      materializeTo[beam.BigEndianLongCoder]

    'a' coderShould roundtripToBytes(Array(97)) and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.ByteCoder]

    "yolo" coderShould roundtripToBytes(Array(121, 111, 108, 111)) and
      beOfType[Beam[_]] and
      materializeTo[beam.StringUtf8Coder]

    4.5 coderShould roundtripToBytes(Array(64, 18, 0, 0, 0, 0, 0, 0)) and
      beOfType[Beam[_]] and
      materializeTo[SDoubleCoder.type] and
      structuralValueConsistentWithEquals() and
      beSerializable() and
      beConsistentWithEquals()

    4.5f coderShould roundtripToBytes(Array(64, -112, 0, 0)) and
      beOfType[Beam[_]] and
      materializeTo[SFloatCoder.type] and
      structuralValueConsistentWithEquals() and
      beSerializable() and
      beConsistentWithEquals()
  }

  it should "support Scala collections" in {
    import scala.collection.BitSet

    val nil: Seq[String] = Nil
    val s: Seq[String] = (1 to 10).map(_.toString)
    val m: Map[String, String] = s.map(v => v -> v).toMap

    nil coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[SeqCoder[_]]

    s coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[SeqCoder[_]] and
      beFullyCompliant()

    s.toList coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[ListCoder[_]] and
      beFullyCompliant()

    s.toVector coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[VectorCoder[_]] and
      beFullyCompliant()

    m coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[MapCoder[_, _]] and
      beFullyCompliantNonDeterministic()

    SortedMap.from(m) coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[SortedMapCoder[_, _]] and
      beFullyCompliant()

    s.toSet coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[SetCoder[_]] and
      beFullyCompliantNonDeterministic()

    mut.ListBuffer.from(s) coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[BufferCoder[_]] and
      beFullyCompliant()

    BitSet.fromSpecific(1 to 100000) coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[BitSetCoder] and
      beFullyCompliant()

    mut.Set.from(s) coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[MutableSetCoder[_]] and
      beFullyCompliant()

    Array("1", "2", "3") coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[ArrayCoder[_]] and
      beFullyCompliantNotConsistentWithEquals()

    {
      implicit val pqOrd: Ordering[String] = Ordering.String.on(_.reverse)
      val pq = new mut.PriorityQueue[String]()(pqOrd)
      pq ++= s

      implicit val pqEq: Equality[mut.PriorityQueue[String]] = {
        case (a: mut.PriorityQueue[String], b: mut.PriorityQueue[_]) => a.toList == b.toList
        case _                                                       => false
      }

      pq coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[MutablePriorityQueueCoder[_]] and
        beSerializable() and
        structuralValueConsistentWithEquals() and
        beNotConsistentWithEquals() and
        bytesCountTested() and
        beNonDeterministic()
    }
  }

  it should "support Scala enumerations" in {
    ScalaColor.Red coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.VarIntCoder] and
      beFullyCompliant()
  }

  it should "support Scala option" in {
    Option(1) coderShould roundtrip() and
      beOfType[Disjunction[_, _]] and
      materializeTo[DisjunctionCoder[_, _]] and
      beFullyCompliant()

    Some(1) coderShould roundtrip() and
      beOfType[Ref[_]] and
      materializeTo[RefCoder[_]] and
      beFullyCompliant()

    None coderShould roundtrip() and
      beOfType[Singleton[_]] and
      materializeTo[SingletonCoder[_]] and
      beFullyCompliant()
  }

  it should "not support inner case classes" in {
    {
      the[Throwable] thrownBy {
        InnerObject coderShould roundtrip()
      }
    }.getMessage should include(
      "Found an $outer field in class com.spotify.scio.coders.CoderTest$$"
    )

    val cw = new ClassWrapper()
    try {
      cw.runWithImplicit
      throw new Throwable("Is expected to throw when passing implicit from outer class")
    } catch {
      case _: NullPointerException =>
      // In this case outer field is called "$cw" and it is hard to wrap it with proper exception
      // so we allow it to fail with NullPointerException
    }

    {
      the[Throwable] thrownBy {
        cw.InnerCaseClass("49") coderShould roundtrip()
      }
    }.getMessage should startWith(
      "Found an $outer field in class com.spotify.scio.coders.CoderTest$$"
    )

    {
      the[Throwable] thrownBy {
        cw.run()
      }
    }.getMessage should startWith(
      "Found an $outer field in class com.spotify.scio.coders.ClassWrapper$$"
    )

    {
      the[Throwable] thrownBy {
        InnerCaseClass("42") coderShould roundtrip()
      }
    }.getMessage should startWith(
      "Found an $outer field in class com.spotify.scio.coders.CoderTest$$"
    )

    case class ClassInsideMethod(str: String)

    {
      the[Throwable] thrownBy {
        ClassInsideMethod("50") coderShould roundtrip()
      }
    }.getMessage should startWith(
      "Found an $outer field in class com.spotify.scio.coders.CoderTest$$"
    )

    {
      the[Throwable] thrownBy {
        InnerObject.InnerCaseClass("42") coderShould roundtrip()
      }
    }.getMessage should startWith(
      "Found an $outer field in class com.spotify.scio.coders.CoderTest$$"
    )
  }

  it should "support inner classes in objects with RefCoder" in {
    TopLevelObject1.InnerCaseClass("42") coderShould roundtrip() and
      beOfType[Ref[_]] and
      materializeTo[RefCoder[_]] and
      beFullyCompliant()
  }

  it should "support tuples" in {
    import shapeless.syntax.std.tuple._
    val t22 = (
      42,
      "foo",
      4.2d,
      4.2f,
      'a',
      List(1, 2, 3, 4, 5),
      42,
      "foo",
      4.2d,
      4.2f,
      'a',
      List(1, 2, 3, 4, 5),
      42,
      "foo",
      4.2d,
      4.2f,
      'a',
      List(1, 2, 3, 4, 5),
      42,
      "foo",
      4.2d,
      4.2f
    )

    t22.take(2) coderShould roundtrip() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[Tuple2Coder[_, _]] and
      beFullyCompliant()
    t22.take(3) coderShould roundtrip()
    t22.take(4) coderShould roundtrip()
    t22.take(5) coderShould roundtrip()
    t22.take(6) coderShould roundtrip()
    t22.take(7) coderShould roundtrip()
    t22.take(8) coderShould roundtrip()
    t22.take(9) coderShould roundtrip()
    t22.take(10) coderShould roundtrip()
    t22.take(11) coderShould roundtrip()
    t22.take(12) coderShould roundtrip()
    t22.take(13) coderShould roundtrip()
    t22.take(14) coderShould roundtrip()
    t22.take(15) coderShould roundtrip()
    t22.take(16) coderShould roundtrip()
    t22.take(17) coderShould roundtrip()
    t22.take(18) coderShould roundtrip()
    t22.take(19) coderShould roundtrip()
    t22.take(20) coderShould roundtrip()
    t22.take(21) coderShould roundtrip()
    t22.take(22) coderShould roundtrip()
  }

  it should "have a Coder for Nothing, Unit, Void" in {
    Coder[Void] coderShould beOfType[Beam[_]] and
      materializeTo[VoidCoder.type] and
      beSerializable() and beDeterministic()

    Coder[Unit] coderShould beOfType[Beam[_]] and
      materializeTo[UnitCoder.type] and
      beSerializable() and beDeterministic()

    val bnc = CoderMaterializer.beamWithDefault[Nothing](Coder[Nothing])
    noException shouldBe thrownBy {
      bnc.asInstanceOf[beam.Coder[Any]].encode(null, null)
    }
    an[IllegalStateException] should be thrownBy {
      bnc.decode(new ByteArrayInputStream(Array()))
    }
  }

  it should "support Java collections" in {
    import java.lang.{Iterable => JIterable}
    import java.util.{
      ArrayList => JArrayList,
      Collection => JCollection,
      List => JList,
      Set => JSet,
      Map => JMap,
      PriorityQueue => JPriorityQueue
    }

    val elems = (1 to 10).map(_.toString)

    {
      val i: JIterable[String] = (elems: Iterable[String]).asJava
      implicit val iEq: Equality[JIterable[String]] = {
        case (xs: JIterable[String], ys: JIterable[String]) =>
          IteratorUtils.toArray(xs.iterator()) sameElements IteratorUtils.toArray(ys.iterator())
        case _ => false
      }

      i coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[beam.IterableCoder[_]] and
        beNotConsistentWithEquals()
    }

    {
      val c: JCollection[String] = elems.asJavaCollection
      implicit val iEq: Equality[JCollection[String]] = {
        case (xs: JCollection[String], ys: JCollection[String]) =>
          IteratorUtils.toArray(xs.iterator()) sameElements IteratorUtils.toArray(ys.iterator())
        case _ => false
      }
      c coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[beam.CollectionCoder[_]] and
        beNotConsistentWithEquals()
    }

    {
      val l: JList[String] = elems.asJava
      l coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[beam.ListCoder[_]] and
        beFullyCompliant()
    }

    {
      val al: JArrayList[String] = new JArrayList(elems.asJava)
      al coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[JArrayListCoder[_]] and
        beFullyCompliant()
    }

    {
      val s: JSet[String] = elems.toSet.asJava
      s coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[beam.SetCoder[_]] and
        structuralValueConsistentWithEquals() and
        beSerializable()
    }

    {
      val m: JMap[String, Int] = (1 to 10)
        .map(v => v.toString -> v)
        .toMap
        .asJava
      m coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[beam.MapCoder[_, _]] and
        beFullyCompliantNonDeterministic()
    }

    {
      implicit val pqOrd: Ordering[String] = Ordering.String.on(_.reverse)
      val pq = new JPriorityQueue[String](pqOrd)
      pq.addAll(elems.asJavaCollection)

      implicit val pqEq: Equality[java.util.PriorityQueue[String]] = {
        case (a: JPriorityQueue[String], b: JPriorityQueue[_]) =>
          a.toArray sameElements b.toArray
        case _ => false
      }

      pq coderShould roundtrip() and
        beOfType[CoderTransform[_, _]] and
        materializeTo[JPriorityQueueCoder[_]] and
        beSerializable() and
        structuralValueConsistentWithEquals() and
        beNonDeterministic()
    }
  }

  it should "Derive serializable coders" in {
    coderIsSerializable[Nothing]
    coderIsSerializable[Int]
    coderIsSerializable[String]
    coderIsSerializable[List[Int]]
    coderIsSerializable(Coder.kryo[Int])
    coderIsSerializable(Coder.gen[(Int, Int)])
    coderIsSerializable(Coder.gen[DummyCC])
    coderIsSerializable[NestedA]
    coderIsSerializable[Top]
    coderIsSerializable[SampleField]
  }

  it should "derive coders for product types" in {
    DummyCC("dummy") coderShould notFallback()
    DummyCC("") coderShould notFallback()
    ParameterizedDummy("dummy") coderShould notFallback()
    MultiParameterizedDummy("dummy", 2) coderShould notFallback()
    user coderShould notFallback()
    (1, "String", List[Int]()) coderShould notFallback()
    val ds = (1 to 10).map(_ => DummyCC("dummy")).toList
    ds coderShould notFallback()
  }

  it should "derive coders for sealed class hierarchies" in {
    val ta: Top = TA(1, "test")
    val tb: Top = TB(4.2)
    ta coderShould roundtrip() and
      beOfType[Disjunction[_, _]] and
      materializeTo[DisjunctionCoder[_, _]] and
      beFullyCompliantNonDeterministic()
    tb coderShould notFallback()
    (123, "hello", ta, tb, List(("bar", 1, "foo"))) coderShould notFallback()
  }

  it should "give informative error when derivation fails" in {
    // scalatest does not give the compiler error message with 'shouldNot compile'
    // invert the test and get the message from the thrown exception
    try {
      "Coder.gen[NotDerivableClass]" should compile: @nowarn
    } catch {
      case e: TestFailedException =>
        e.getMessage should include(
          "in parameter 't' of product type com.spotify.scio.coders.NotDerivableClass"
        )
    }
  }

  // FIXME: TableRowJsonCoder cannot be tested in scio-test because of circular dependency on scio-google-cloud-platform
  it should "support all the already supported types" in {
    import java.math.{BigInteger, BigDecimal => jBigDecimal}
    import java.nio.file.FileSystems

    ByteString.copyFromUtf8("SampleString") coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[ByteStringCoder] and
      beFullyCompliant()

    BigInt("1234") coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.BigIntegerCoder] and
      beFullyCompliant()

    BigDecimal("1234") coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.BigDecimalCoder] and
      beFullyCompliant()

    UUID.randomUUID() coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[Tuple2Coder[_, _]] and
      beFullyCompliant()

    FileSystems.getDefault.getPath("logs", "access.log") coderShould notFallback() and
      beFullyCompliant()

    val bs = new java.util.BitSet()
    (1 to 100000).foreach(x => bs.set(x))
    bs coderShould notFallback() and
      beFullyCompliant()

    new BigInteger("123456789") coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[beam.BigIntegerCoder] and
      beFullyCompliant()

    new jBigDecimal("123456789.98765") coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[beam.BigDecimalCoder] and
      beFullyCompliant()

    // java time
    Instant.now() coderShould notFallback()
    LocalTime.now() coderShould notFallback()
    LocalDate.now() coderShould notFallback()
    LocalTime.now() coderShould notFallback()
    LocalDateTime.now() coderShould notFallback()
    Duration.ofSeconds(123) coderShould notFallback()
    Period.ofDays(123) coderShould notFallback()

    // java sql
    java.sql.Timestamp
      .valueOf("1971-02-03 04:05:06.789") coderShould roundtrip() and beOfType[Transform[_, _]] and
      beFullyCompliant()
    java.sql.Date.valueOf("1971-02-03") coderShould roundtrip() and beOfType[Transform[_, _]] and
      beFullyCompliant()
    java.sql.Time.valueOf("01:02:03") coderShould roundtrip() and beOfType[Transform[_, _]] and
      beFullyCompliant()

    // joda time
    val now = org.joda.time.Instant.now()
    now coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[beam.InstantCoder] and
      beFullyCompliant()
    new org.joda.time.DateTime() coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[JodaDateTimeCoder] and
      beFullyCompliant()
    new org.joda.time.LocalDate() coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[JodaLocalDateCoder] and
      beFullyCompliant()
    new org.joda.time.LocalTime coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[JodaLocalTimeCoder] and
      beFullyCompliant()
    new org.joda.time.LocalDateTime coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[JodaLocalDateTimeCoder] and
      beFullyCompliant()
    new org.joda.time.DateTime coderShould roundtrip() and beOfType[Beam[_]] and
      materializeTo[JodaDateTimeCoder] and
      beFullyCompliant()
    new org.joda.time.Duration(123) coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[BigEndianLongCoder] and
      beFullyCompliant()
  }

  it should "support IntervalWindow" in {
    import org.apache.beam.sdk.transforms.windowing.IntervalWindow
    val now = org.joda.time.Instant.now()
    new IntervalWindow(now.minus(4000), now) coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[IntervalWindow.IntervalWindowCoder] and
      beFullyCompliant()
  }

  it should "support GlobalWindow" in {
    import org.apache.beam.sdk.transforms.windowing.GlobalWindow
    GlobalWindow.INSTANCE coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[GlobalWindow.Coder] and
      beFullyCompliant()
  }

  it should "support java's Instant" in {
    // Support full nano range
    Instant.ofEpochSecond(0, 123123123) coderShould notFallback() and
      beFullyCompliant()
    Instant.MIN coderShould notFallback()
    Instant.MAX coderShould notFallback()
    Instant.EPOCH coderShould notFallback()
    Instant.now coderShould notFallback()
  }

  it should "support Beam Row" in {
    import java.lang.{Double => jDouble, Integer => jInt, String => jString}

    import org.apache.beam.sdk.schemas.{Schema => bSchema}
    import org.apache.beam.sdk.values.Row

    val beamSchema =
      bSchema
        .builder()
        .addInt32Field("c1")
        .addStringField("c2")
        .addDoubleField("c3")
        .build()

    implicit val coderRow: Coder[Row] = Coder.row(beamSchema)
    List[(jInt, jString, jDouble)]((1, "row", 1.0), (2, "row", 2.0), (3, "row", 3.0))
      .map { case (a, b, c) =>
        Row.withSchema(beamSchema).addValues(a, b, c).build()
      }
      .foreach { r =>
        r coderShould roundtrip() and
          beOfType[Beam[_]] and
          materializeTo[beam.RowCoder] and
          beFullyCompliantNonDeterministic()
      }
  }

  it should "support Scala objects" in {
    TopLevelObject coderShould roundtrip() and
      beOfType[Singleton[_]] and
      materializeTo[SingletonCoder[_]] and
      beSerializable() and
      beConsistentWithEquals() and
      beDeterministic()

    TopLevelObject1 coderShould roundtrip() and
      beOfType[Singleton[_]]
  }

  it should "only derive Coder if no coder exists" in {
    CaseClassWithExplicitCoder(1, "hello") coderShould notFallback() and haveCoderInstance(
      CaseClassWithExplicitCoder.caseClassWithExplicitCoderCoder
    )
  }

  it should "derive when protobuf Any is in scope" in {
    """import com.google.protobuf.Any
      |Coder.gen[DummyCC]
      |""".stripMargin should compile
  }

  it should "support classes with private constructors" in {
    import com.spotify.scio.coders.kryo.{fallback => f}
    PrivateClass(42L) coderShould fallback() and materializeTo[KryoAtomicCoder[_]]
  }

  it should "support classes that contain classes with private constructors" in {
    import com.spotify.scio.coders.kryo._
    UsesPrivateClass(PrivateClass(1L)) coderShould notFallback()
  }

  it should "not derive Coders for org.apache.beam.sdk.values.Row" in {
    "Coder[Row]" shouldNot compile
    "Coder.gen[Row]" shouldNot compile
  }

  it should "have a nice verifyDeterministic exception for pairs" in {
    val caught =
      intercept[NonDeterministicException] {
        val coder = Coder[(Double, Double)]

        materialize(coder).verifyDeterministic()
      }

    val expectedMsg =
      "Tuple2Coder(_1 -> DoubleCoder, _2 -> DoubleCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include("field _1 is using non-deterministic DoubleCoder")
    caught.getMessage should include("field _2 is using non-deterministic DoubleCoder")
  }

  it should "have a nice verifyDeterministic exception for case classes" in {
    val caught =
      intercept[NonDeterministicException] {
        val coder = Coder[NonDeterministic]

        materialize(coder).verifyDeterministic()
      }

    val expectedMsg =
      "RecordCoder[com.spotify.scio.coders.NonDeterministic](a -> DoubleCoder, b -> DoubleCoder)" +
        " is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include("field a is using non-deterministic DoubleCoder")
    caught.getMessage should include("field b is using non-deterministic DoubleCoder")
  }

  it should "have a nice verifyDeterministic exception for disjunctions" in {
    val caught =
      intercept[NonDeterministicException] {
        val coder = Coder[Either[Double, Int]]

        materialize(coder).verifyDeterministic()
      }

    // field names vary between Scala 2.11 and 2.12
    val leftCoder = materialize(Coder[scala.util.Left[Double, Int]]).toString
    val rightCoder = materialize(Coder[scala.util.Right[Double, Int]]).toString

    val expectedMsg =
      s"DisjunctionCoder[scala.util.Either](false -> $leftCoder, true -> $rightCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include(s"case false is using non-deterministic $leftCoder")
  }

  it should "support protobuf messages" in {
    "Coder[OuterClassForProto.ProtoComplexMessage]" should compile
    val b = OuterClassForProto.ProtoComplexMessage.newBuilder
    val ex = b.setArtistGid("1").setTimeFilter(OuterClassForProto.EnumExample.OPT1).build()

    ex coderShould notFallback()
    ClassWithProtoEnum(
      "somestring",
      OuterClassForProto.EnumExample.OPT1
    ) coderShould roundtrip() and beOfType[Ref[_]]
  }

  it should "support java enums" in {
    JavaEnumExample.GOOD_THING coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.StringUtf8Coder] and
      beFullyCompliant()
    JavaEnumExample.BAD_THING coderShould roundtrip()
  }

  it should "#1604: not throw on null" in {
    import java.lang.{
      Double => jDouble,
      Float => jFloat,
      Integer => jInt,
      Long => jLong,
      Short => jShort
    }

    val opts: PipelineOptions = PipelineOptionsFactory.create()
    opts.as(classOf[ScioOptions]).setNullableCoders(true)

    null.asInstanceOf[String] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[String] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[jInt] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[jFloat] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[jDouble] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[jLong] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[jShort] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[(String, Top)] coder WithOptions(opts) should notFallback()
    (null, null).asInstanceOf[(String, Top)] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[DummyCC] coder WithOptions(opts) should notFallback()
    DummyCC(null) coder WithOptions(opts) should notFallback()
    null.asInstanceOf[Top] coder WithOptions(opts) should notFallback()
    null.asInstanceOf[Either[String, Int]] coder WithOptions(opts) should notFallback()

    type T = (String, Int, Top)
    val example: T = ("Hello", 42, TA(1, "World"))
    val nullExample1: T = ("Hello", 42, TA(1, null))
    val nullExample2: T = ("Hello", 42, null)
    val nullExample3: T = null
    example coder WithOptions(opts) should notFallback()
    nullExample1 coder WithOptions(opts) should notFallback()
    nullExample2 coder WithOptions(opts) should notFallback()
    nullExample3 coder WithOptions(opts) should notFallback()

    val nullBCoder = CoderMaterializer.beamWithDefault(Coder[T], o = opts)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample1)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample2)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample3)

    val noopObserver: org.apache.beam.sdk.util.common.ElementByteSizeObserver = (_: Long) => ()
    nullBCoder.registerByteSizeObserver(nullExample1, noopObserver)
    nullBCoder.registerByteSizeObserver(nullExample2, noopObserver)
    nullBCoder.registerByteSizeObserver(nullExample3, noopObserver)
  }

  it should "not re-wrap nullable coder" in {
    val opts: PipelineOptions = PipelineOptionsFactory.create()
    opts.as(classOf[ScioOptions]).setNullableCoders(true)

    val bCoder = beam.NullableCoder.of(beam.StringUtf8Coder.of())
    // this could be a PairSCollectionFunctions.valueCoder extracted after materialization
    val coder = Coder.beam(bCoder)
    val materializedCoder = CoderMaterializer.beamWithDefault(coder, opts)
    materializedCoder shouldBe new MaterializedCoder[String](bCoder)
  }

  it should "derive zstd coders when configured" in {
    val tmp = writeZstdBytes(Array[Byte](7, 6, 5, 4, 3, 2, 1, 0))
    val opts = zstdOpts("com.test.ZstdTestCaseClass", s"file://${tmp.getAbsolutePath}")
    ZstdTestCaseClass(1, "s", 10L) coder WithOptions(opts) should notFallback() and
      beOfType[Ref[_]] and
      materializeTo[ZstdCoder[_]] and
      beFullyCompliant()
  }

  it should "derive a zstd coder for the value side of a 2-tuple" in {
    val tmp = writeZstdBytes(Array[Byte](7, 6, 5, 4, 3, 2, 1, 0))
    val opts = zstdOpts("com.test.ZstdTestCaseClass", s"file://${tmp.getAbsolutePath}")
    ("Foo", ZstdTestCaseClass(1, "s", 10L)) coder WithOptions(opts) should notFallback() and
      beOfType[CoderTransform[_, _]] and
      materializeTo[Tuple2Coder[_, _]] and
      beFullyCompliant() and { ctx =>
        // casts checked in materializeTo
        val valueCoder =
          ctx.beamCoder.asInstanceOf[MaterializedCoder[_]].bcoder.asInstanceOf[Tuple2Coder[_, _]].bc
        valueCoder shouldBe a[ZstdCoder[_]]
      }
  }

  it should "not derive zstd coders when not configured" in {
    ZstdTestCaseClass(1, "s", 10L) coderShould notFallback() and
      beOfType[Ref[_]] and
      materializeTo[RefCoder[_]] and
      beFullyCompliant()
  }

  it should "have a useful stacktrace when a Coder throws" in {
    val ok = SampleField("foo", RecordType(List(SampleField("bar", IntegerType))))
    val nok = SampleField("foo", RecordType(List(SampleField(null, IntegerType))))

    implicit lazy val c: Coder[SampleField] = Coder.gen[SampleField]
    ok coderShould roundtrip()
    val caught = intercept[beam.CoderException] {
      nok coderShould roundtrip()
    }

    val stackTrace = caught.getStackTrace
    stackTrace should contain(CoderStackTrace.CoderStackElemMarker)
    stackTrace.count(_ == CoderStackTrace.CoderStackElemMarker) shouldBe 1
    val materializationStackTrace = stackTrace.dropWhile(_ != CoderStackTrace.CoderStackElemMarker)
    materializationStackTrace.map(_.getFileName) should contain("CoderTest.scala")
  }

  it should "#1651: remove all annotations from derived coders" in {
    Coder[TraitWithAnnotation] coderShould beSerializable()
  }

  it should "Serialize Java beans using a Schema Coder" in {
    val javaUser = new com.spotify.scio.bean.UserBean("Julien", 33)
    javaUser coderShould notFallback() and
      beOfType[Beam[_]] and
      materializeTo[SchemaCoder[_]] and
      beFullyCompliant()
  }

  it should "Serialize WrappedArray using wrappedArrayCoder" in {
    val wrappedArray: mut.WrappedArray[String] = Array("foo", "bar")
    wrappedArray coderShould notFallback() and beFullyCompliantNotConsistentWithEquals()
  }

  it should "support derivation of recursive types" in {
    noException should be thrownBy
      SerializableUtils.serializeToByteArray(CoderMaterializer.beamWithDefault(Coder[Top]))

    noException should be thrownBy
      SerializableUtils.serializeToByteArray(
        CoderMaterializer.beamWithDefault(Coder[SampleFieldType])
      )

    Coder[SampleFieldType] coderShould beSerializable() and
      beConsistentWithEquals() and
      beDeterministic()

    // https://github.com/spotify/scio/issues/3707
    SampleField(
      "hello",
      StringType
    ) coderShould roundtrip() and beFullyCompliant()

    SampleField(
      "hello",
      RecordType(
        List(SampleField("record", RecordType(List.empty)), SampleField("int", IntegerType))
      )
    ) coderShould roundtrip() and beFullyCompliant()
  }

  it should "#2595: work with parameterized types" in {
    case class Example(stringT: Either[Array[Byte], String], longT: Either[Array[Byte], Long])
    val c = CoderMaterializer.beamWithDefault(implicitly[Coder[Example]])
    c.encode(Example(Right("str"), Right(0L)), System.out)
  }

  it should "#2467 support derivation of directly recursive types" in {
    Recursive(1, Option(Recursive(2, None))) coderShould notFallback()
  }

  it should "#2644 verifyDeterministic throw a NonDeterministicException exception for Set" in {
    an[NonDeterministicException] should be thrownBy {
      val coder = Coder[Set[Int]]
      materialize(coder).verifyDeterministic()
    }
  }

  it should "support GenericJson types" in {
    val tableSchema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
        new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
      ).asJava
    )

    tableSchema coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.StringUtf8Coder] and
      beFullyCompliant()
  }

  it should "optimize for AnyVal" in {
    AnyValExample("dummy") coderShould beOfType[Transform[_, _]] and
      materializeToTransformOf[beam.StringUtf8Coder] and
      structuralValueConsistentWithEquals() and
      beSerializable() and
      beConsistentWithEquals() and
      beDeterministic()
  }

  it should "support Algebird's Moments" in {
    new Moments(0.0, 0.0, 0.0, 0.0, 0.0) coderShould roundtrip()
    Moments(12) coderShould roundtrip() and
      beOfType[Transform[_, _]] and
      materializeToTransformOf[Tuple5Coder[_, _, _, _, _]] and
      beFullyCompliantNonDeterministic()
  }

  it should "return different hashCodes for different instances of parameterized Coders" in {
    def hashCodesAreDifferent[T1, T2](c1: Coder[T1], c2: Coder[T2]): Assertion = {
      val hashCodeThis = CoderMaterializer.beamWithDefault(c1).hashCode()
      val hashCodeThat = CoderMaterializer.beamWithDefault(c2).hashCode()

      hashCodeThis shouldNot equal(hashCodeThat)
    }

    hashCodesAreDifferent(Coder[(String, Int)], Coder[(String, String)])
    hashCodesAreDifferent(Coder[Map[String, Int]], Coder[Map[String, String]])
    hashCodesAreDifferent(Coder[List[String]], Coder[List[Int]])
    hashCodesAreDifferent(Coder[Option[String]], Coder[Option[Int]])
    hashCodesAreDifferent(
      Coder.xmap[String, Int](Coder[String])(_.toInt, _.toString),
      Coder.xmap[Int, String](Coder[Int])(_.toString, _.toInt)
    )
  }

  it should "support Guava Bloom Filters" in {
    import com.google.common.hash.{BloomFilter, Funnels}

    implicit val funnel = Funnels.stringFunnel(Charset.forName("UTF-8"))
    val bloomFilter = BloomFilter.create[String](funnel, 5L)

    bloomFilter coderShould roundtrip() and
      beOfType[Beam[_]] and
      materializeTo[GuavaBloomFilterCoder[_]] and
      structuralValueConsistentWithEquals() and
      beNotConsistentWithEquals() and
      bytesCountTested() and
      beDeterministic()
  }

  it should "not serialize any magnolia internals after materialization" in {
    class ObjectOutputStreamInspector extends ObjectOutputStream(NullOutputStream.INSTANCE) {
      private val classes = Set.newBuilder[String]

      override def writeClassDescriptor(desc: ObjectStreamClass): Unit = {
        classes += desc.getName
        super.writeClassDescriptor(desc)
      }

      def serializedClasses: Set[String] = {
        super.flush()
        super.close()
        classes.result()
      }
    }

    val inspector = new ObjectOutputStreamInspector()
    // case class
    inspector.writeObject(CoderMaterializer.beamWithDefault(Coder[DummyCC]))
    // sealed trait
    inspector.writeObject(CoderMaterializer.beamWithDefault(Coder[Top]))

    inspector.serializedClasses should not contain oneOf(
      classOf[magnolia1.CaseClass[Coder, _]].getName,
      classOf[magnolia1.Param[Coder, _]].getName,
      classOf[magnolia1.SealedTrait[Coder, _]].getName,
      classOf[magnolia1.Subtype[Coder, _]].getName
    )
  }

  /*
   * Case class nested inside another class. Do not move outside
   * */
  case class InnerCaseClass(str: String)

  /*
   * Object nested inside another class. Do not move outside
   * */
  object InnerObject {
    case class InnerCaseClass(str: String)
  }
}

// enumeration
object ScalaColor extends Enumeration {
  val Red, Green, Blue = Value
}

// record
final case class UserId(bytes: Seq[Byte])
final case class User(id: UserId, username: String, email: String)

// disjunction
sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(aDouble: Double) extends Top

// case classes
final case class DummyCC(s: String)
final case class ParameterizedDummy[A](value: A)
final case class MultiParameterizedDummy[A, B](valuea: A, valueb: B)

// objects
object TopLevelObject
object TopLevelObject1 {
  val somestring = "something"
  val somelong = 42L
  case class InnerCaseClass(str: String)
}

// explicit coder
case class CaseClassWithExplicitCoder(i: Int, s: String)
object CaseClassWithExplicitCoder {
  import org.apache.beam.sdk.coders.{AtomicCoder, StringUtf8Coder, VarIntCoder}

  import java.io.{InputStream, OutputStream}
  implicit val caseClassWithExplicitCoderCoder: Coder[CaseClassWithExplicitCoder] =
    Coder.beam(new AtomicCoder[CaseClassWithExplicitCoder] {
      val sc = StringUtf8Coder.of()
      val ic = VarIntCoder.of()
      def encode(value: CaseClassWithExplicitCoder, os: OutputStream): Unit = {
        ic.encode(value.i, os)
        sc.encode(value.s, os)
      }
      def decode(is: InputStream): CaseClassWithExplicitCoder = {
        val i = ic.decode(is)
        val s = sc.decode(is)
        CaseClassWithExplicitCoder(i, s)
      }
    })
}

// nested
case class NestedB(x: Int)
case class NestedA(nb: NestedB)

// recursive
case class SampleField(name: String, fieldType: SampleFieldType)
sealed trait SampleFieldType
case object IntegerType extends SampleFieldType
case object StringType extends SampleFieldType
case class RecordType(fields: List[SampleField]) extends SampleFieldType
case class Recursive(a: Int, rec: Option[Recursive] = None)

// private
class PrivateClass private (val value: Long) extends AnyVal
object PrivateClass {
  def apply(l: Long): PrivateClass = new PrivateClass(l)
}
case class UsesPrivateClass(privateClass: PrivateClass)

// non derivable
trait NotDerivableTrait
case class NotDerivableClass(s: String, t: NotDerivableTrait)

// proto
case class ClassWithProtoEnum(s: String, `enum`: OuterClassForProto.EnumExample)

// serial UID
@SerialVersionUID(1)
sealed trait TraitWithAnnotation
@SerialVersionUID(2)
final case class FirstImplementationWithAnnotation(s: String) extends TraitWithAnnotation
@SerialVersionUID(3)
final case class SecondImplementationWithAnnotation(i: Int) extends TraitWithAnnotation

// AnyVal
final case class AnyValExample(value: String) extends AnyVal

// Non deterministic
final case class NonDeterministic(a: Double, b: Double)

class ClassWrapper() {
  case class InnerCaseClass(str: String)

  def runWithImplicit(implicit
    c: Coder[InnerCaseClass]
  ): Unit =
    InnerCaseClass("51") coderShould roundtrip()

  def run(): Unit =
    InnerCaseClass("51") coderShould roundtrip()
}
