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

import java.{lang => jl, util => ju}
import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.coders.CoderTestUtils._
import com.spotify.scio.testing.CoderAssertions._
import com.spotify.scio.testing.PipelineSpec
import com.twitter.chill.{java => _, _}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values.KV
import org.joda.time.Instant

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.apache.beam.sdk.testing.CoderProperties

case class RecordA(name: String, value: Int)
case class RecordB(name: String, value: Int)

class KryoAtomicCoderTest extends PipelineSpec {
  type CoderFactory = () => BCoder[Any]
  val cf: () => KryoAtomicCoder[Any] = () => new KryoAtomicCoder[Any](KryoOptions())

  "KryoAtomicCoder" should "assert that it is not deterministic and not consistent with equals" in {
    Pair("record", 10) kryoCoderShould beNonDeterministic() and beNotConsistentWithEquals()
  }

  it should "support Scala collections" in {
    Seq(1, 2, 3) kryoCoderShould roundtrip()
    List(1, 2, 3) kryoCoderShould roundtrip()
    Set(1, 2, 3) kryoCoderShould roundtrip()
    Map("a" -> 1, "b" -> 2, "c" -> 3) kryoCoderShould roundtrip()
  }

  it should "support Scala tuples" in {
    ("hello", 10) kryoCoderShould roundtrip()
    ("hello", 10, 10.0) kryoCoderShould roundtrip()
    ("hello", (10, 10.0)) kryoCoderShould roundtrip()
  }

  it should "support Scala case classes" in {
    Pair("record", 10) kryoCoderShould roundtrip()
  }

  // Enable once https://github.com/scala/scala/pull/10425 is release
  ignore should "support wrapped iterables" in {
    // handle immutable underlying Java collections
    val list = List(1, 2, 3).asJava

    // Iterable/Collection should have proper equality
    list.asInstanceOf[jl.Iterable[Int]].asScala kryoCoderShould roundtrip()
    list.asInstanceOf[ju.Collection[Int]].asScala kryoCoderShould roundtrip()
    list.asScala kryoCoderShould roundtrip()
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord(1)
    r kryoCoderShould roundtrip()
    ("key", r) kryoCoderShould roundtrip()
    CaseClassWithGenericRecord("record", 10, r) kryoCoderShould roundtrip()
  }

  it should "support Avro SpecificRecord" in {
    val r = newSpecificRecord(1)
    r kryoCoderShould roundtrip()
    ("key", r) kryoCoderShould roundtrip()
    CaseClassWithSpecificRecord("record", 10, r) kryoCoderShould roundtrip()
  }

  it should "support KV" in {
    KV.of("key", 1.0) kryoCoderShould roundtrip()
    KV.of("key", (10, 10.0)) kryoCoderShould roundtrip()
    KV.of("key", newSpecificRecord(1)) kryoCoderShould roundtrip()
    KV.of("key", newGenericRecord(1)) kryoCoderShould roundtrip()
  }

  it should "support Instant" in {
    Instant.now() kryoCoderShould roundtrip()
  }

  it should "support TableRow" in {
    val r = new TableRow().set("repeated_field", List("a", "b").asJava)
    r kryoCoderShould roundtrip()
  }

  it should "support large objects" in {
    val vs = Iterable((1 to 1000000).map("value-%08d".format(_)): _*)
    val kv = ("key", vs)
    kv kryoCoderShould roundtrip()
  }

  it should "support BigDecimal" in {
    val bigDecimal = BigDecimal(1000.42)
    bigDecimal kryoCoderShould roundtrip()
  }

  it should "support custom KryoRegistrar" in {
    val c = cf()

    // should use custom serializer in annotated RecordAKryoRegistrar
    val a = CoderUtils.encodeToByteArray(c, RecordA("foo", 10))
    CoderUtils.decodeFromByteArray(c, a) shouldBe RecordA("foo", 20)

    // should use default serializer since RecordBKryoRegistrar is not annotated
    val b = CoderUtils.encodeToByteArray(c, RecordB("foo", 10))
    CoderUtils.decodeFromByteArray(c, b) shouldBe RecordB("foo", 10)

    // custom serializer should be more space efficient
    a.length should be < b.length

    // class name does not end with KryoRegistrar
    "@KryoRegistrar class Foo extends IKryoRegistrar {}" shouldNot compile

    // class does not extend IKryoRegistrar
    "@KryoRegistrar class FooKryoRegistrar extends Product {}" shouldNot compile
  }

  it should "support BoundedWindow" in {
    import org.apache.beam.sdk.transforms.windowing.BoundedWindow
    case class TestBoundedWindow(
      x: Int,
      override val maxTimestamp: Instant = BoundedWindow.TIMESTAMP_MAX_VALUE
    ) extends BoundedWindow

    TestBoundedWindow(777) kryoCoderShould roundtrip() and
      beOfType[Fallback[_]] and
      materializeTo[KryoAtomicCoder[_]] and
      beNonDeterministic() and
      beNotConsistentWithEquals()
  }

  it should "support kryo registration required option" in {
    val options = PipelineOptionsFactory
      .fromArgs("--kryoRegistrationRequired=true")
      .create()
    val sc = ScioContext(options)

    implicit def alwaysUseKryo[A: ClassTag]: Coder[A] = Coder.kryo[A]

    sc.parallelize(1 to 10).map(x => RecordB(x.toString, x))

    val e = the[PipelineExecutionException] thrownBy { sc.run() }

    val msg = "Class is not registered: com.spotify.scio.coders.RecordB"
    e.getCause.getMessage should startWith(msg)
  }

  it should "support kryo registrar with custom options" in {
    implicit val recordBfallbackCoder = Coder.kryo[RecordB]
    // ensure we get a different kryo instance from object pool.
    val options = PipelineOptionsFactory
      .fromArgs("--kryoReferenceTracking=false", "--kryoRegistrationRequired=false")
      .create()
    val sc = ScioContext(options)
    sc.parallelize(1 to 10).map(x => RecordB(x.toString, x))

    val e = the[PipelineExecutionException] thrownBy { sc.run() }

    val msg = "Class is not registered: com.spotify.scio.coders.RecordB"
    e.getCause.getMessage should startWith(msg)
  }

  it should "support registerByteSizeObserver" in {
    val c = cf()
    val s: Seq[String] = (1 to 10).map(_.toString)
    // Check that registerByteSizeObserver() and encode() are consistent
    CoderProperties.testByteCount(c, BCoder.Context.OUTER, Array[Object](s))
    CoderProperties.testByteCount(
      c,
      BCoder.Context.OUTER,
      Array[Object](s.map(x => (x, s.asJava)))
    )
  }
}

@KryoRegistrar
class RecordAKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.forClass(new KSerializer[RecordA] {
      override def write(k: Kryo, output: Output, obj: RecordA): Unit = {
        output.writeString(obj.name)
        output.writeInt(obj.value)
      }

      override def read(kryo: Kryo, input: Input, tpe: Class[RecordA]): RecordA =
        RecordA(input.readString(), input.readInt() + 10)
    })
    ()
  }
}

class RecordBKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.forClass(new KSerializer[RecordB] {
      override def write(k: Kryo, output: Output, obj: RecordB): Unit = {
        output.writeString(obj.name)
        output.writeInt(obj.value)
      }

      override def read(kryo: Kryo, input: Input, tpe: Class[RecordB]): RecordB =
        RecordB(input.readString(), input.readInt() + 10)
    })
    ()
  }
}

// Dummy registrar that when reference tracing disabled requires registration
@KryoRegistrar
class TestOverridableKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit =
    if (!k.getReferences && !k.isRegistrationRequired) {
      // Overrides the value set from KryoOptions
      k.setRegistrationRequired(true)
    }
}
