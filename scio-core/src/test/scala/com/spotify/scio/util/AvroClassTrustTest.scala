/*
 * Copyright 2026 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.util {

  import com.spotify.scio.ScioContext
  import com.spotify.scio.options.AvroOptions
  import com.spotify.scio.util.avrotrust.byoption.OptionPojo
  import com.spotify.scio.util.avrotrust.byinitializer.InitializerPojo
  import com.spotify.scio.util.avrotrust.byapi.ApiPojo
  import com.spotify.scio.util.avrotrust.untrusted.UntrustedPojo
  import com.spotify.scio.util.avrotrust.collections.CollectionsPojo
  import org.apache.avro.io.{DecoderFactory, EncoderFactory}
  import org.apache.avro.reflect.{ReflectData, ReflectDatumReader, ReflectDatumWriter}
  import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
  import org.scalatest.flatspec.AnyFlatSpec
  import org.scalatest.matchers.should.Matchers

  import java.io.ByteArrayOutputStream
  import scala.reflect.ClassTag

  class AvroClassTrustTest extends AnyFlatSpec with Matchers {

    private def roundtrip[T: ClassTag](value: T): T = {
      val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val schema = ReflectData.get().getSchema(cls)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      new ReflectDatumWriter[T](schema).write(value, encoder)
      encoder.flush()
      new ReflectDatumReader[T](schema)
        .read(null.asInstanceOf[T], DecoderFactory.get().binaryDecoder(out.toByteArray, null))
    }

    // ClassSecurityValidator only exists in avro 1.12.1+; older avro trusts every class
    private val hasValidator =
      scala.util.Try(Class.forName("org.apache.avro.util.ClassSecurityValidator")).isSuccess

    private def assertUntrusted(): Unit =
      if (hasValidator) a[SecurityException] should be thrownBy roundtrip(new UntrustedPojo("a"))

    "AvroClassTrust" should "not trust reflect records outside the trusted packages" in {
      assume(hasValidator, "avro has no ClassSecurityValidator")
      AvroClassTrust.install()
      assertUntrusted()
    }

    it should "trust packages from --avroTrustedPackages when creating a ScioContext" in {
      val (opts, _) =
        ScioContext.parseArguments[PipelineOptions](
          Array("--avroTrustedPackages=com.spotify.scio.util.avrotrust.byoption")
        )
      ScioContext(opts)
      roundtrip(new OptionPojo("a")).name shouldBe "a"
      assertUntrusted()
    }

    it should "trust packages from --avroTrustedPackages on workers" in {
      val options = PipelineOptionsFactory
        .fromArgs("--avroTrustedPackages=com.spotify.scio.util.avrotrust.byinitializer")
        .as(classOf[AvroOptions])
      new AvroClassTrustInitializer().beforeProcessing(options)
      roundtrip(new InitializerPojo("a")).name shouldBe "a"
    }

    it should "trust the default classes, and arrays of them, only" in {
      assume(hasValidator, "avro has no ClassSecurityValidator")
      AvroClassTrust.install()
      val validator =
        Class
          .forName("org.apache.avro.util.ClassSecurityValidator")
          .getMethod("getGlobal")
          .invoke(null)
      val isTrusted = Class
        .forName("org.apache.avro.util.ClassSecurityValidator$ClassSecurityPredicate")
        .getMethod("isTrusted", classOf[Class[_]])
      def trusted(c: Class[_]): Boolean = isTrusted.invoke(validator, c).asInstanceOf[Boolean]

      AvroClassTrust.DefaultTrustedClasses.foreach { name =>
        withClue(name)(trusted(Class.forName(name)) shouldBe true)
      }
      // array java-class props, e.g. [I and [Ljava.lang.String;
      trusted(classOf[Array[Int]]) shouldBe true
      trusted(classOf[Array[Byte]]) shouldBe true
      trusted(classOf[Array[String]]) shouldBe true
      trusted(classOf[Array[Array[Short]]]) shouldBe true
      // in packages avro 1.11.4 to 1.12.0 trusted, but constructible from strings in hostile data.
      // Not java.io.File or java.net.URL: scio's test JVMs trust them with SERIALIZABLE_CLASSES,
      // see project/JavaOptions.scala
      trusted(classOf[java.io.FileOutputStream]) shouldBe false
      trusted(classOf[java.io.FileInputStream]) shouldBe false
      trusted(classOf[java.net.Socket]) shouldBe false
      trusted(classOf[java.lang.ProcessBuilder]) shouldBe false
      trusted(classOf[Array[java.io.FileOutputStream]]) shouldBe false
      trusted(classOf[java.util.concurrent.ConcurrentHashMap[_, _]]) shouldBe false
    }

    it should "trust reflect records with BigDecimal and collection fields" in {
      AvroClassTrust.trustPackages("com.spotify.scio.util.avrotrust.collections")
      val value = new CollectionsPojo(
        new java.math.BigDecimal("1.50"),
        java.util.Arrays.asList("a", "b"),
        java.util.Collections.singletonMap("k", java.lang.Long.valueOf(1L))
      )
      val read = roundtrip(value)
      read.amount shouldBe value.amount
      read.list shouldBe value.list
      read.map shouldBe value.map
    }

    it should "trust packages given to trustPackages" in {
      AvroClassTrust.trustPackages("com.spotify.scio.util.avrotrust.byapi")
      roundtrip(new ApiPojo("a")).name shouldBe "a"
    }
  }
}

// reflect records need a no-arg constructor
package com.spotify.scio.util.avrotrust.byoption {
  class OptionPojo(var name: String) { def this() = this(null) }
}
package com.spotify.scio.util.avrotrust.byinitializer {
  class InitializerPojo(var name: String) { def this() = this(null) }
}
package com.spotify.scio.util.avrotrust.byapi {
  class ApiPojo(var name: String) { def this() = this(null) }
}
package com.spotify.scio.util.avrotrust.collections {
  class CollectionsPojo(
    var amount: java.math.BigDecimal,
    var list: java.util.List[String],
    var map: java.util.Map[String, java.lang.Long]
  ) { def this() = this(null, null, null) }
}
package com.spotify.scio.util.avrotrust.untrusted {
  class UntrustedPojo(var name: String) { def this() = this(null) }
}
