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
import org.apache.beam.sdk.coders.{Coder => BCoder, CoderException}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.beam.sdk.util.SerializableUtils

import scala.jdk.CollectionConverters._
import scala.collection.{mutable => mut}
import java.io.{ByteArrayInputStream, ObjectOutputStream, ObjectStreamClass}
import org.apache.beam.sdk.testing.CoderProperties
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.options.ScioOptions
import com.twitter.algebird.Moments
import org.apache.commons.io.output.NullOutputStream
import org.scalatest.Assertion

import java.nio.charset.Charset
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

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
object TestObject
object TestObject1 {
  val somestring = "something"
  val somelong = 42L
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

// proto
case class ClassWithProtoEnum(s: String, enum: OuterClassForProto.EnumExample)

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

final class CoderTest extends AnyFlatSpec with Matchers {
  val userId: UserId = UserId(Array[Byte](1, 2, 3, 4))
  val user: User = User(userId, "johndoe", "johndoe@spotify.com")

  def materialize[T](coder: Coder[T]): BCoder[T] =
    CoderMaterializer.beam(PipelineOptionsFactory.create(), coder)

  "Coders" should "support primitives" in {
    1 coderShould roundtrip()
    'a' coderShould roundtrip()
    "yolo" coderShould roundtrip()
    4.5 coderShould roundtrip()
  }

  it should "support Scala collections" in {
    import scala.collection.BitSet

    val nil: Seq[String] = Nil
    val s: Seq[String] = (1 to 10).map(_.toString)
    val m: Map[String, String] = s.map(v => v -> v).toMap

    nil coderShould notFallback()
    s coderShould notFallback()
    s.toList coderShould notFallback()
    s.toVector coderShould notFallback()
    m coderShould notFallback()
    s.toSet coderShould notFallback()
    mut.ListBuffer(1 to 10: _*) coderShould notFallback()
    None coderShould notFallback()
    Option(1) coderShould notFallback()
    Some(1) coderShould notFallback()
    BitSet(1 to 100000: _*) coderShould notFallback()

    Right(1) coderShould notFallback()
    Left(1) coderShould notFallback()
    mut.Set(s: _*) coderShould notFallback()

    val bsc = CoderMaterializer.beamWithDefault(Coder[Seq[String]])
    // Check that registerByteSizeObserver() and encode() are consistent
    CoderProperties.testByteCount(bsc, BCoder.Context.OUTER, Array(s))
    CoderProperties.structuralValueConsistentWithEquals(bsc, s, s)

    val bmc = CoderMaterializer.beamWithDefault(Coder[Map[String, String]])
    CoderProperties.testByteCount(bmc, BCoder.Context.OUTER, Array(m))
    CoderProperties.structuralValueConsistentWithEquals(bmc, m, m)
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

    t22.take(2) coderShould roundtrip()
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

  it should "have a Coder for Nothing" in {
    val bnc = CoderMaterializer.beamWithDefault[Nothing](Coder[Nothing])
    bnc
      .asInstanceOf[BCoder[Any]]
      .encode(null, null) shouldBe () // make sure the code does nothing
    an[IllegalStateException] should be thrownBy {
      bnc.decode(new ByteArrayInputStream(Array()))
    }
  }

  it should "support Java collections" in {
    import java.util.{List => jList, Map => jMap, ArrayList => jArrayList}
    val is = 1 to 10
    val s: jList[String] = is.map(_.toString).asJava
    val m: jMap[String, Int] = is
      .map(v => v.toString -> v)
      .toMap
      .asJava
    val arrayList = new jArrayList(s)

    s coderShould notFallback()
    m coderShould notFallback()
    arrayList coderShould notFallback()
  }

  object Avro {

    import com.spotify.scio.avro.{Account, Address, User => AvUser}

    val accounts: List[Account] = List(new Account(1, "type", "name", 12.5, null))
    val address =
      new Address("street1", "street2", "city", "state", "01234", "Sweden")
    val user = new AvUser(1, "lastname", "firstname", "email@foobar.com", accounts.asJava, address)

    val eq: Equality[GenericRecord] = (a: GenericRecord, b: Any) => a.toString === b.toString
  }

  it should "Derive serializable coders" in {
    coderIsSerializable[Nothing]
    coderIsSerializable[Int]
    coderIsSerializable[String]
    coderIsSerializable[List[Int]]
    coderIsSerializable(Coder.kryo[Int])
    coderIsSerializable(Coder.gen[(Int, Int)])
    coderIsSerializable(Coder.gen[DummyCC])
    coderIsSerializable[com.spotify.scio.avro.User]
    coderIsSerializable[NestedA]
    coderIsSerializable[Top]
    coderIsSerializable[SampleField]
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
    val ds = (1 to 10).map(_ => DummyCC("dummy")).toList
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

    // TableRowJsonCoder
    // SpecificRecordBase
    // Message
    // ByteString
    BigDecimal("1234") coderShould notFallback()
    new java.sql.Timestamp(1) coderShould notFallback()
    new org.joda.time.Instant coderShould notFallback()
    new org.joda.time.LocalDate coderShould notFallback()
    new org.joda.time.LocalTime coderShould notFallback()
    new org.joda.time.LocalDateTime coderShould notFallback()
    new org.joda.time.DateTime coderShould notFallback()
    FileSystems.getDefault.getPath("logs", "access.log") coderShould notFallback()

    "Coder[Void]" should compile
    "Coder[Unit]" should compile

    val bs = new java.util.BitSet()
    (1 to 100000).foreach(x => bs.set(x))
    bs coderShould notFallback()

    new BigInteger("123456789") coderShould notFallback()
    new jBigDecimal("123456789.98765") coderShould notFallback()
    val now = org.joda.time.Instant.now()
    new IntervalWindow(now.minus(4000), now) coderShould notFallback()
  }

  it should "Serialize java's Instant" in {
    // Support full nano range
    Instant.ofEpochSecond(0, 123123123) coderShould notFallback()
    Instant.MIN coderShould notFallback()
    Instant.MAX coderShould notFallback()
    Instant.EPOCH coderShould notFallback()
    Instant.now coderShould notFallback()
  }

  it should "Serialize Row" in {
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
      .foreach(r => r coderShould notFallback())
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

  it should "support classes that contain classes with private constructors" in {
    Coder.gen[UsesPrivateClass]
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
    ClassWithProtoEnum("somestring", OuterClassForProto.EnumExample.OPT1) coderShould notFallback()
  }

  it should "support java enums" in {
    JavaEnumExample.GOOD_THING coderShould roundtrip()
    JavaEnumExample.BAD_THING coderShould roundtrip()
  }

  it should "be deterministic for java enums" in {
    materialize(Coder[JavaEnumExample]).verifyDeterministic()
  }

  it should "support specific fixed data" in {
    val bytes = (0 to 15).map(_.toByte).toArray
    val specificFixed = new FixedSpecificDataExample(bytes)
    specificFixed coderShould beDeterministic()
    specificFixed coderShould roundtrip()
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

    null.asInstanceOf[String] coderShould roundtrip(opts)
    null.asInstanceOf[jInt] coderShould roundtrip(opts)
    null.asInstanceOf[jFloat] coderShould roundtrip(opts)
    null.asInstanceOf[jDouble] coderShould roundtrip(opts)
    null.asInstanceOf[jLong] coderShould roundtrip(opts)
    null.asInstanceOf[jShort] coderShould roundtrip(opts)
    null.asInstanceOf[(String, Top)] coderShould roundtrip(opts)
    (null, null).asInstanceOf[(String, Top)] coderShould roundtrip(opts)
    null.asInstanceOf[DummyCC] coderShould roundtrip(opts)
    DummyCC(null) coderShould roundtrip(opts)
    null.asInstanceOf[Top] coderShould roundtrip(opts)
    null.asInstanceOf[Either[String, Int]] coderShould roundtrip(opts)

    type T = (String, Int, Top)
    val example: T = ("Hello", 42, TA(1, "World"))
    val nullExample1: T = ("Hello", 42, TA(1, null))
    val nullExample2: T = ("Hello", 42, null)
    val nullExample3: T = null
    example coderShould roundtrip(opts)
    nullExample1 coderShould roundtrip(opts)
    nullExample2 coderShould roundtrip(opts)
    nullExample3 coderShould roundtrip(opts)

    val nullBCoder = CoderMaterializer.beamWithDefault(Coder[T], o = opts)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample1)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample2)
    nullBCoder.isRegisterByteSizeObserverCheap(nullExample3)

    val noopObserver: org.apache.beam.sdk.util.common.ElementByteSizeObserver = (_: Long) => ()
    nullBCoder.registerByteSizeObserver(nullExample1, noopObserver)
    nullBCoder.registerByteSizeObserver(nullExample2, noopObserver)
    nullBCoder.registerByteSizeObserver(nullExample3, noopObserver)
  }

  it should "have a useful stacktrace when a Coder throws" in {
    val ok = SampleField("foo", RecordType(List(SampleField("bar", IntegerType))))
    val nok = SampleField("foo", RecordType(List(SampleField(null, IntegerType))))

    implicit lazy val c: Coder[SampleField] = Coder.gen[SampleField]
    ok coderShould roundtrip()
    val caught = intercept[CoderException] {
      nok coderShould roundtrip()
    }

    val stackTrace = caught.getStackTrace
    stackTrace should contain(CoderStackTrace.CoderStackElemMarker)
    stackTrace.count(_ == CoderStackTrace.CoderStackElemMarker) shouldBe 1
    val materializationStackTrace = stackTrace.dropWhile(_ != CoderStackTrace.CoderStackElemMarker)
    materializationStackTrace.map(_.getFileName) should contain("CoderTest.scala")
  }

  it should "#1651: remove all annotations from derived coders" in {
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
    noException should be thrownBy
      SerializableUtils.serializeToByteArray(CoderMaterializer.beamWithDefault(Coder[Top]))

    noException should be thrownBy
      SerializableUtils.serializeToByteArray(
        CoderMaterializer.beamWithDefault(Coder[SampleFieldType])
      )

    "Coder[SampleField]" should compile
    "Coder[SampleFieldType]" should compile

    SampleField("hello", StringType) coderShould roundtrip()

    // https://github.com/spotify/scio/issues/3707
    SampleField("hello", StringType) coderShould beConsistentWithEquals()
    SampleField("hello", StringType) coderShould beDeterministic()

    SampleField(
      "hello",
      RecordType(
        List(SampleField("record", RecordType(List.empty)), SampleField("int", IntegerType))
      )
    ) coderShould roundtrip()
  }

  it should "#2595: work with parameterized types" in {
    case class Example(stringT: Either[Array[Byte], String], longT: Either[Array[Byte], Long])
    val sc = com.spotify.scio.ScioContext.forTest()
    val c = CoderMaterializer.beam(sc, implicitly[Coder[Example]])
    c.encode(Example(Right("str"), Right(0L)), System.out)
  }

  it should "#2467 support derivation of directly recursive types" in {
    Recursive(1, Option(Recursive(2, None))) coderShould roundtrip()
  }

  it should "#2644 verifyDeterministic throw a NonDeterministicException exception for Set" in {
    an[NonDeterministicException] should be thrownBy {
      val coder = Coder[Set[Int]]
      materialize(coder).verifyDeterministic()
    }
  }

  it should "support GenericJson types" in {
    coderIsSerializable[TableSchema]

    val tableSchema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
        new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
      ).asJava
    )

    tableSchema coderShould roundtrip()
  }

  it should "optimize for AnyVal" in {
    coderIsSerializable[AnyValExample]
    Coder[AnyValExample] shouldBe a[Transform[String, AnyValExample]]
  }

  it should "optimize for objects" in {
    coderIsSerializable[TestObject.type]
    Coder[TestObject.type] shouldBe a[Singleton[_]]
  }

  it should "support Algebird's Moments" in {
    coderIsSerializable[Moments]
    new Moments(0.0, 0.0, 0.0, 0.0, 0.0) coderShould roundtrip()
    Moments(12) coderShould roundtrip()
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

    // For transform, even if parameters are equal, hashCodes must be different
    hashCodesAreDifferent(
      Coder.xmap[String, LocalDate](Coder[String])(
        LocalDate.parse(_, DateTimeFormatter.ISO_LOCAL_DATE),
        _.toString
      ),
      Coder.xmap[String, LocalDate](Coder[String])(
        LocalDate.parse(_, DateTimeFormatter.ISO_WEEK_DATE),
        _.toString
      )
    )
  }

  it should "support Guava Bloom Filters" in {
    import com.google.common.hash.{BloomFilter, Funnels}

    implicit val funnel = Funnels.stringFunnel(Charset.forName("UTF-8"))
    val bloomFilter = BloomFilter.create(funnel, 5L)

    bloomFilter coderShould roundtrip()
    bloomFilter coderShould beDeterministic()
  }

  it should "not serialize any magnolia internals after materialization" in {
    class ObjectOutputStreamInspector
        extends ObjectOutputStream(NullOutputStream.NULL_OUTPUT_STREAM) {
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
}
