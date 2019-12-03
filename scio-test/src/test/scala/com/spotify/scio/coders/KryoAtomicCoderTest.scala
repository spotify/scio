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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class RecordA(name: String, value: Int)
case class RecordB(name: String, value: Int)

class KryoAtomicCoderTest extends PipelineSpec {
  type CoderFactory = () => BCoder[Any]
  val cf = () => new KryoAtomicCoder[Any](KryoOptions())

  "KryoAtomicCoder" should "support Scala collections" in {
    Seq(1, 2, 3) coderShould roundtripKryo()
    List(1, 2, 3) coderShould roundtripKryo()
    Set(1, 2, 3) coderShould roundtripKryo()
    Map("a" -> 1, "b" -> 2, "c" -> 3) coderShould roundtripKryo()
  }

  it should "support Scala tuples" in {
    ("hello", 10) coderShould roundtripKryo()
    ("hello", 10, 10.0) coderShould roundtripKryo()
    ("hello", (10, 10.0)) coderShould roundtripKryo()
  }

  it should "support Scala case classes" in {
    Pair("record", 10) coderShould roundtripKryo()
  }

  it should "support wrapped iterables" in {
    // handle immutable underlying Java collections
    val list = List(1, 2, 3).asJava

    // Iterable/Collection should have proper equality
    list.asInstanceOf[jl.Iterable[Int]].asScala coderShould roundtripKryo()
    list.asInstanceOf[ju.Collection[Int]].asScala coderShould roundtripKryo()
    list.asScala coderShould roundtripKryo()
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord(1)
    r coderShould roundtripKryo()
    ("key", r) coderShould roundtripKryo()
    CaseClassWithGenericRecord("record", 10, r) coderShould roundtripKryo()
  }

  it should "support Avro SpecificRecord" in {
    val r = newSpecificRecord(1)
    r coderShould roundtripKryo()
    ("key", r) coderShould roundtripKryo()
    CaseClassWithSpecificRecord("record", 10, r) coderShould roundtripKryo()
  }

  it should "support KV" in {
    KV.of("key", 1.0) coderShould roundtripKryo()
    KV.of("key", (10, 10.0)) coderShould roundtripKryo()
    KV.of("key", newSpecificRecord(1)) coderShould roundtripKryo()
    KV.of("key", newGenericRecord(1)) coderShould roundtripKryo()
  }

  it should "support Instant" in {
    Instant.now() coderShould roundtripKryo()
  }

  it should "support TableRow" in {
    val r = new TableRow().set("repeated_field", List("a", "b").asJava)
    r coderShould roundtripKryo()
  }

  it should "support large objects" in {
    val vs = Iterable((1 to 1000000).map("value-%08d".format(_)): _*)
    val kv = ("key", vs)
    kv coderShould roundtripKryo()
  }

  it should "support BigDecimal" in {
    val bigDecimal = BigDecimal(1000.42)
    bigDecimal coderShould roundtripKryo()
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
