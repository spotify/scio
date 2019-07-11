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

import com.spotify.scio.proto.OuterClassForProto
import com.spotify.scio.testing.CoderAssertions._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.{Coder => BCoder, CoderRegistry}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalactic.Equality
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.{mutable => mut}

final case class UserId(bytes: Seq[Byte])
final case class User(id: UserId, username: String, email: String)

sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(anDouble: Double) extends Top

case class DummyCC(s: String)
case class ParameterizedDummy[A](value: A)
case class MultiParameterizedDummy[A, B](valuea: A, valueb: B)
object TestObject
object TestObject1 {
  val somestring = "something"
  val somelong = 42L
}
case class CaseClassWithExplicitCoder(i: Int, s: String)
object CaseClassWithExplicitCoder {
  import org.apache.beam.sdk.coders.{AtomicCoder, StringUtf8Coder, VarIntCoder}
  import java.io.{InputStream, OutputStream}
  implicit val caseClassWithExplicitCoderCoder =
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

case class NestedB(x: Int)
case class NestedA(nb: NestedB)

class PrivateClass private (val value: Long) extends AnyVal
object PrivateClass {
  def apply(l: Long): PrivateClass = new PrivateClass(l)
}

case class ClassWithProtoEnum(s: String, enum: OuterClassForProto.EnumExample)

@SerialVersionUID(1)
sealed trait TraitWithAnnotation
@SerialVersionUID(2)
final case class FirstImplementationWithAnnotation(s: String) extends TraitWithAnnotation
@SerialVersionUID(3)
final case class SecondImplementationWithAnnotation(i: Int) extends TraitWithAnnotation

class CodersTest extends FlatSpec with Matchers {

  val userId = UserId(Array[Byte](1, 2, 3, 4))
  val user = User(userId, "johndoe", "johndoe@spotify.com")

  def materialize[T](coder: Coder[T]): BCoder[T] = {
    CoderMaterializer
      .beam(
        CoderRegistry.createDefault(),
        PipelineOptionsFactory.create(),
        coder
      )
  }

  "Coders" should "support primitives" in {
    1 coderShould roundtrip()
    "yolo" coderShould roundtrip()
    4.5 coderShould roundtrip()
  }

  it should "support Scala collections" in {
    import scala.collection.BitSet

    val nil: Seq[String] = Nil
    val s: Seq[String] = (1 to 10).toSeq.map(_.toString)
    val m = s.map { v =>
      v.toString -> v
    }.toMap

    nil coderShould notFallback()
    s coderShould notFallback()
    s.toList coderShould notFallback()
    s.toVector coderShould notFallback()
    m coderShould notFallback()
    s.toSet coderShould notFallback()
    mut.ListBuffer((1 to 10): _*) coderShould notFallback()
    None coderShould notFallback()
    Option(1) coderShould notFallback()
    Some(1) coderShould notFallback()
    BitSet(1 to 100000: _*) coderShould notFallback()

    Right(1) coderShould notFallback()
    Left(1) coderShould notFallback()
  }

  it should "support Java collections" in {
    import java.util.{List => jList, Map => jMap, ArrayList => jArrayList}
    val is = (1 to 10).toSeq
    val s: jList[String] = is.map(_.toString).asJava
    val m: jMap[String, Int] = is
      .map { v =>
        v.toString -> v
      }
      .toMap
      .asJava
    val arrayList = new jArrayList(s)

    s coderShould notFallback()
    m coderShould notFallback()
    arrayList coderShould notFallback()
  }

  object Avro {
    import com.spotify.scio.avro.{User => AvUser, Account, Address}

    val accounts: List[Account] = List(new Account(1, "tyoe", "name", 12.5))
    val address =
      new Address("street1", "street2", "city", "state", "01234", "Sweden")
    val user = new AvUser(1, "lastname", "firstname", "email@foobar.com", accounts.asJava, address)

    val eq = new Equality[GenericRecord] {
      def areEqual(a: GenericRecord, b: Any): Boolean =
        a.toString === b.toString // YOLO
    }
  }

  it should "Derive serializable coders" in {
    coderIsSerializable[Int]
    coderIsSerializable[String]
    coderIsSerializable[List[Int]]
    coderIsSerializable(Coder.kryo[Int])
    coderIsSerializable(Coder.gen[(Int, Int)])
    coderIsSerializable(Coder.gen[DummyCC])
    coderIsSerializable[com.spotify.scio.avro.User]
    coderIsSerializable[NestedA]
  }

  it should "support Avro's SpecificRecordBase" in {
    Avro.user coderShould notFallback()
  }

  it should "support Avro's GenericRecord" in {
    val schema = Avro.user.getSchema
    val record: GenericRecord = Avro.user

    implicit val c: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)
    implicit val eq: Equality[GenericRecord] = Avro.eq

    record coderShould notFallback()
  }

  it should "derive coders for product types" in {
    DummyCC("dummy") coderShould notFallback()
    DummyCC("") coderShould notFallback()
    ParameterizedDummy("dummy") coderShould notFallback()
    MultiParameterizedDummy("dummy", 2) coderShould notFallback()
    user coderShould notFallback()
    (1, "String", List[Int]()) coderShould notFallback()
    val ds = (1 to 10).map { _ =>
      DummyCC("dummy")
    }.toList
    ds coderShould notFallback()
  }

  it should "derive coders for sealed class hierarchies" in {
    val ta: Top = TA(1, "test")
    val tb: Top = TB(4.2)
    ta coderShould notFallback()
    tb coderShould notFallback()
    (123, "hello", ta, tb, List(("bar", 1, "foo"))) coderShould notFallback()
  }

  // FIXME: implement the missing coders
  it should "support all the already supported types" in {
    import java.math.{BigInteger, BigDecimal => jBigDecimal}
    import java.nio.file.FileSystems

    import org.apache.beam.sdk.transforms.windowing.IntervalWindow
    import org.joda.time._

    // TableRowJsonCoder
    // SpecificRecordBase
    // Message
    // ByteString
    BigDecimal("1234") coderShould notFallback()
    new Instant coderShould notFallback()
    new LocalDate coderShould notFallback()
    new LocalTime coderShould notFallback()
    new LocalDateTime coderShould notFallback()
    new DateTime coderShould notFallback()
    FileSystems.getDefault().getPath("logs", "access.log") coderShould notFallback()

    "Coder[Void]" should compile
    "Coder[Unit]" should compile

    import java.util.BitSet
    val bs = new BitSet()
    (1 to 100000).foreach { x =>
      bs.set(x)
    }
    bs coderShould notFallback()

    new BigInteger("123456789") coderShould notFallback()
    new jBigDecimal("123456789.98765") coderShould notFallback()
    new IntervalWindow((new Instant).minus(4000), new Instant) coderShould notFallback()
  }

  it should "Serialize java's Instant" in {
    import java.time.{Instant => jInstant}

    // Both thow exceptions but they should be unusual enough to not be an issue
    // jInstant.MIN coderShould notFallback()
    // jInstant.MAX coderShould notFallback()
    jInstant.EPOCH coderShould notFallback()
    jInstant.now coderShould notFallback()
  }

  // Broken because of a bug in Beam
  // See: https://issues.apache.org/jira/browse/BEAM-5645
  ignore should "Serialize Row (see: BEAM-5645)" in {
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

    implicit val coderRow = Coder.row(beamSchema)
    val rows =
      List[(jInt, jString, jDouble)]((1, "row", 1.0), (2, "row", 2.0), (3, "row", 3.0))
        .map {
          case (a, b, c) =>
            Row.withSchema(beamSchema).addValues(a, b, c).build()
        }
        .foreach { r =>
          r coderShould notFallback()
        }
  }

  it should "Serialize objects" in {
    TestObject coderShould notFallback()
    TestObject1 coderShould notFallback()
  }

  it should "only derive Coder if no coder exists" in {
    CaseClassWithExplicitCoder(1, "hello") coderShould notFallback()
    Coder[CaseClassWithExplicitCoder] should
      ===(CaseClassWithExplicitCoder.caseClassWithExplicitCoderCoder)
  }

  it should "provide a fallback if no safe coder is available" in {
    val record: GenericRecord = Avro.user
    record coderShould fallback()
  }

  it should "support classes with private constructors" in {
    Coder.gen[PrivateClass]
    PrivateClass(42L) coderShould fallback()
  }

  it should "not derive Coders for org.apache.beam.sdk.values.Row" in {
    import org.apache.beam.sdk.values.Row
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
      "PairCoder(_1 -> DoubleCoder, _2 -> DoubleCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include("field _1 is using non-deterministic DoubleCoder")
    caught.getMessage should include("field _2 is using non-deterministic DoubleCoder")
  }

  it should "have a nice verifyDeterministic exception for case classes" in {
    val caught =
      intercept[NonDeterministicException] {
        val coder = Coder[(Double, Double, Double)]

        materialize(coder).verifyDeterministic()
      }

    val expectedMsg =
      "RecordCoder[scala.Tuple3](_1 -> DoubleCoder, _2 -> DoubleCoder, _3 -> DoubleCoder)" +
        " is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include("field _1 is using non-deterministic DoubleCoder")
    caught.getMessage should include("field _2 is using non-deterministic DoubleCoder")
    caught.getMessage should include("field _3 is using non-deterministic DoubleCoder")
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

    val expectedMsg = s"DisjunctionCoder[scala.util.Either](" +
      s"id -> BooleanCoder, false -> $leftCoder, true -> $rightCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include(s"case false is using non-deterministic $leftCoder")
  }

  it should "support protobuf messages" in {
    "Coder[OuterClassForProto.ProtoComplexMessage]" should compile
    val b = OuterClassForProto.ProtoComplexMessage.newBuilder
    val ex = b.setArtistGid("1").setTimeFilter(OuterClassForProto.EnumExample.OPT1).build()
    ex coderShould notFallback()
    ClassWithProtoEnum("somestring", OuterClassForProto.EnumExample.OPT1) coderShould notFallback()
  }

  it should "support java enums" in {
    JavaEnumExample.GOOD_THING coderShould roundtrip()
    JavaEnumExample.BAD_THING coderShould roundtrip()
  }

  it should "support specific fixed data" in {
    val bytes = (0 to 15).map(_.toByte).toArray
    new FixedSpecificDataExample(bytes) coderShould roundtrip()
  }

  it should "#1604: not throw on null" in {
    import java.lang.{
      Double => jDouble,
      Float => jFloat,
      Integer => jInt,
      Long => jLong,
      Short => jShort
    }

    def opts: PipelineOptions =
      PipelineOptionsFactory
        .fromArgs("--nullableCoders=true")
        .create()

    null.asInstanceOf[String] coderShould roundtrip(opts)
    null.asInstanceOf[jInt] coderShould roundtrip(opts)
    null.asInstanceOf[jFloat] coderShould roundtrip(opts)
    null.asInstanceOf[jDouble] coderShould roundtrip(opts)
    null.asInstanceOf[jLong] coderShould roundtrip(opts)
    null.asInstanceOf[jShort] coderShould roundtrip(opts)
    (null, null).asInstanceOf[(String, String)] coderShould roundtrip(opts)
    DummyCC(null) coderShould roundtrip(opts)

    type T = (String, Int, Top)
    val example: T = ("Hello", 42, TA(1, "World"))
    val nullExample1: T = ("Hello", 42, TA(1, null))
    val nullExample2: T = ("Hello", 42, null)
    example coderShould roundtrip(opts)
    nullExample1 coderShould roundtrip(opts)
    nullExample2 coderShould roundtrip(opts)

    val nullBCoder = CoderMaterializer.beamWithDefault(Coder[T], o = opts)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample1)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample2)

    val noopObserver =
      new org.apache.beam.sdk.util.common.ElementByteSizeObserver {
        def reportElementSize(s: Long) = ()
      }

    nullBCoder.registerByteSizeObserver(nullExample1, noopObserver)
    nullBCoder.registerByteSizeObserver(nullExample2, noopObserver)

    import com.spotify.scio.avro.TestRecord
    val record: GenericRecord = TestRecord
      .newBuilder()
      .setStringField(null)
      .build()

    val nullExample3 = (record, record.get("string_field"))
    nullExample3 coderShould roundtrip(opts)
  }

  it should "have a useful stacktrace when a Coder throws" in {
    val ok: (String, String) = ("foo", "bar")
    val nok: (String, String) = (null, "bar")
    ok coderShould roundtrip()
    val caught =
      intercept[RuntimeException] {
        nok coderShould roundtrip()
      }

    caught.getStackTrace.find(_.getClassName.contains(classOf[CodersTest].getName)) shouldNot be(
      None
    )
  }

  it should "#1651: remove all anotations from derived coders" in {
    coderIsSerializable[TraitWithAnnotation]
  }

  it should "Serialize Java beans using a Schema Coder" in {
    val javaUser = new com.spotify.scio.bean.UserBean("Julien", 33)
    javaUser coderShould roundtrip()
    javaUser coderShould notFallback()
  }

  it should "Serialize WrappedArray using wrappedArrayCoder" in {
    val wrappedArray: mut.WrappedArray[String] = Array("foo", "bar")
    wrappedArray coderShould notFallback()
  }

  it should "support derivation of recursive types" in {
    case class SampleField(name: String, fieldType: SampleFieldType)
    sealed trait SampleFieldType
    case object IntegerType extends SampleFieldType
    case object StringType extends SampleFieldType
    case class RecordType(fields: List[SampleField]) extends SampleFieldType

    "Coder[SampleField]" should compile
    "Coder[SampleFieldType]" should compile

    SampleField("hello", StringType) coderShould roundtrip()

    SampleField(
      "hello",
      RecordType(
        List(SampleField("record", RecordType(List.empty)), SampleField("int", IntegerType))
      )
    ) coderShould roundtrip()
  }
}
