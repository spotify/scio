/*
 * Copyright 2017 Spotify AB.
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

import scala.collection.JavaConverters._
import scala.collection.{mutable => mut}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalatest.{Assertion, FlatSpec, Matchers}
import scala.reflect.{classTag, ClassTag}

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

import com.spotify.scio.proto._
case class ClassWithProtoEnum(s: String, enum: OuterClassForProto.EnumExample)

class CodersTest extends FlatSpec with Matchers {

  val userId = UserId(Array[Byte](1, 2, 3, 4))
  val user = User(userId, "johndoe", "johndoe@spotify.com")

  private def checkSer[A](implicit c: Coder[A]) = {
    val beamCoder = CoderMaterializer.beamWithDefault(c)
    org.apache.beam.sdk.util.SerializableUtils.ensureSerializable(beamCoder)
  }

  import org.scalactic.Equality
  def check[T](t: T, options: PipelineOptions = PipelineOptionsFactory.create())(
    implicit C: Coder[T],
    eq: Equality[T]): Assertion = {
    val beamCoder = CoderMaterializer.beamWithDefault(C, o = options)
    org.apache.beam.sdk.util.SerializableUtils.ensureSerializable(beamCoder)
    val enc = CoderUtils.encodeToByteArray(beamCoder, t)
    val dec = CoderUtils.decodeFromByteArray(beamCoder, enc)
    dec should ===(t)
  }

  def checkNotFallback[T: ClassTag](t: T)(implicit C: Coder[T], eq: Equality[T]): Assertion = {
    C should !==(Coder.kryo[T])
    check[T](t)(C, eq)
  }

  def checkFallback[T: ClassTag](t: T)(implicit C: Coder[T], eq: Equality[T]): Assertion = {
    C should ===(Coder.kryo[T])
    check[T](t)(C, eq)
  }

  def materialize[T](coder: Coder[T]): BCoder[T] = {
    CoderMaterializer
      .beam(
        CoderRegistry.createDefault(),
        PipelineOptionsFactory.create(),
        coder
      )
  }

  "Coders" should "support primitives" in {
    check(1)
    check("yolo")
    check(4.5)
  }

  it should "support Scala collections" in {
    import scala.collection.BitSet

    val nil: Seq[String] = Nil
    val s: Seq[String] = (1 to 10).toSeq.map(_.toString)
    val m = s.map { v =>
      v.toString -> v
    }.toMap

    checkNotFallback(nil)
    checkNotFallback(s)
    checkNotFallback(s.toList)
    checkNotFallback(s.toVector)
    checkNotFallback(m)
    checkNotFallback(s.toSet)
    checkNotFallback(mut.ListBuffer((1 to 10): _*))
    checkNotFallback(None)
    checkNotFallback(Option(1))
    checkNotFallback(Some(1))
    checkNotFallback(BitSet(1 to 100000: _*))
  }

  it should "support Java collections" in {
    import java.util.{List => jList, Map => jMap}
    val is = (1 to 10).toSeq
    val s: jList[String] = is.map(_.toString).asJava
    val m: jMap[String, Int] = is
      .map { v =>
        v.toString -> v
      }
      .toMap
      .asJava
    checkNotFallback(s)
    checkNotFallback(m)
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
    checkSer[Int]
    checkSer[String]
    checkSer[List[Int]]
    checkSer(Coder.kryo[Int])
    checkSer(Coder.gen[(Int, Int)])
    checkSer(Coder.gen[DummyCC])
    checkSer[com.spotify.scio.avro.User]
    checkSer[NestedA]
  }

  it should "support Avro's SpecificRecordBase" in {
    checkNotFallback(Avro.user)
  }

  it should "support Avro's GenericRecord" in {
    val schema = Avro.user.getSchema
    val record: GenericRecord = Avro.user
    checkNotFallback(record)(classTag[GenericRecord], Coder.avroGenericRecordCoder(schema), Avro.eq)
  }

  it should "derive coders for product types" in {
    checkNotFallback(DummyCC("dummy"))
    checkNotFallback(DummyCC(""))
    checkNotFallback(ParameterizedDummy("dummy"))
    checkNotFallback(MultiParameterizedDummy("dummy", 2))
    checkNotFallback(user)
    checkNotFallback((1, "String", List[Int]()))
    val ds = (1 to 10).map { _ =>
      DummyCC("dummy")
    }.toList
    checkNotFallback(ds)
  }

  it should "derive coders for sealed class hierarchies" in {
    val ta: Top = TA(1, "test")
    val tb: Top = TB(4.2)
    checkNotFallback(ta)
    checkNotFallback(tb)
    checkNotFallback((123, "hello", ta, tb, List(("bar", 1, "foo"))))
  }

  // FIXME: implement the missing coders
  it should "support all the already supported types" in {
    import org.joda.time._
    import java.nio.file.FileSystems
    import java.math.{BigInteger, BigDecimal => jBigDecimal}
    import org.apache.beam.sdk.transforms.windowing.IntervalWindow

    // TableRowJsonCoder
    // SpecificRecordBase
    // Message
    // ByteString
    checkNotFallback(BigDecimal("1234"))
    checkNotFallback(new Instant)
    checkNotFallback(new LocalDate)
    checkNotFallback(new LocalTime)
    checkNotFallback(new LocalDateTime)
    checkNotFallback(new DateTime)
    checkNotFallback(FileSystems.getDefault().getPath("logs", "access.log"))

    "Coder[Void]" should compile
    "Coder[Unit]" should compile

    import java.util.BitSet
    val bs = new BitSet()
    (1 to 100000).foreach { x =>
      bs.set(x)
    }
    checkNotFallback(bs)

    checkNotFallback(new BigInteger("123456789"))
    checkNotFallback(new jBigDecimal("123456789.98765"))
    checkNotFallback(new IntervalWindow((new Instant).minus(4000), new Instant))

  }

  it should "Serialize java's Instant" in {
    import java.time.{Instant => jInstant}

    // Both thow exceptions but they should be unusual enough to not be an issue
    // checkNotFallback(jInstant.MIN)
    // checkNotFallback(jInstant.MAX)
    checkNotFallback(jInstant.EPOCH)
    checkNotFallback(jInstant.now)
  }

  // Broken because of a bug in Beam
  // See: https://issues.apache.org/jira/browse/BEAM-5645
  ignore should "Serialize Row (see: BEAM-5645)" in {
    import org.apache.beam.sdk.schemas.{Schema => bSchema}
    import org.apache.beam.sdk.values.Row
    import java.lang.{Integer => jInt, String => jString, Double => jDouble}

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
          checkNotFallback(r)
        }
  }

  it should "Serialize objects" in {
    checkNotFallback(TestObject)
    checkNotFallback(TestObject1)
  }

  it should "only derive Coder if no coder exists" in {
    checkNotFallback(CaseClassWithExplicitCoder(1, "hello"))
    Coder[CaseClassWithExplicitCoder] should
      ===(CaseClassWithExplicitCoder.caseClassWithExplicitCoderCoder)
  }

  it should "provide a fallback if no safe coder is available" in {
    val record: GenericRecord = Avro.user
    checkFallback(record)
  }

  it should "support classes with private constructors" in {
    Coder.gen[PrivateClass]
    checkFallback(PrivateClass(42L))
  }

  it should "not derive Coders for org.apache.beam.sdk.values.Row" in {
    import org.apache.beam.sdk.values.Row
    "Coder[Row]" shouldNot compile
    "Coder.gen[Row]" shouldNot compile
  }

  it should "have a nice verifyDeterministic exception for case classes" in {
    val caught =
      intercept[NonDeterministicException] {
        val coder = Coder[(Double, Double)]

        materialize(coder).verifyDeterministic()
      }

    val expectedMsg =
      "RecordCoder[scala.Tuple2](_1 -> DoubleCoder, _2 -> DoubleCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include("field _1 is using non-deterministic DoubleCoder")
    caught.getMessage should include("field _2 is using non-deterministic DoubleCoder")
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
      s"id -> VarIntCoder, 0 -> $leftCoder, 1 -> $rightCoder) is not deterministic"

    caught.getMessage should startWith(expectedMsg)
    caught.getMessage should include(s"case 0 is using non-deterministic $leftCoder")
  }

  it should "support protobuf messages" in {
    import com.spotify.scio.proto._
    "Coder[OuterClassForProto.ProtoComplexMessage]" should compile
    val b = OuterClassForProto.ProtoComplexMessage.newBuilder
    val ex = b.setArtistGid("1").setTimeFilter(OuterClassForProto.EnumExample.OPT1).build()
    checkNotFallback(ex)
    checkNotFallback(ClassWithProtoEnum("somestring", OuterClassForProto.EnumExample.OPT1))
  }

  it should "support java enums" in {
    check(JavaEnumExample.GOOD_THING)
    check(JavaEnumExample.BAD_THING)
  }

  it should "support specific fixed data" in {
    val bytes = (0 to 15).map(_.toByte).toArray
    check(new FixedSpefificDataExample(bytes))
  }

  it should "#1447: Properly serde spanner's ReadOperation" in {
    import org.apache.beam.sdk.io.gcp.spanner.ReadOperation
    val ro = ReadOperation.create().withQuery("SELECT 1")
    check(ro)
  }

  it should "support spanner's Mutation class" in {
    import com.google.cloud.spanner.Mutation
    check(Mutation.newInsertBuilder("myTable").set("foo").to("bar").build())
  }

  it should "support spanner's Struct class" in {
    import com.google.cloud.spanner.Struct
    check(Struct.newBuilder().set("foo").to("bar").build())
  }

  it should "#1604: not throw on null" in {
    import java.lang.{
      Integer => jInt,
      Float => jFloat,
      Double => jDouble,
      Long => jLong,
      Short => jShort
    }

    def opts: PipelineOptions =
      PipelineOptionsFactory
        .fromArgs("--nullableCoders=true")
        .create()

    check[String](null, opts)
    check[jInt](null, opts)
    check[jFloat](null, opts)
    check[jDouble](null, opts)
    check[jLong](null, opts)
    check[jShort](null, opts)
    check[(String, String)]((null, null), opts)
    check(DummyCC(null), opts)
  }
}
