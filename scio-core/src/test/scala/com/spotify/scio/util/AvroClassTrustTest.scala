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

    "AvroClassTrust" should "not trust reflect records outside the trusted packages" in {
      AvroClassTrust.install()
      a[SecurityException] should be thrownBy roundtrip(new UntrustedPojo("a"))
    }

    it should "trust packages from --avroTrustedPackages when creating a ScioContext" in {
      val (opts, _) =
        ScioContext.parseArguments[PipelineOptions](
          Array("--avroTrustedPackages=com.spotify.scio.util.avrotrust.byoption")
        )
      ScioContext(opts)
      roundtrip(new OptionPojo("a")).name shouldBe "a"
      a[SecurityException] should be thrownBy roundtrip(new UntrustedPojo("a"))
    }

    it should "trust packages from --avroTrustedPackages on workers" in {
      val options = PipelineOptionsFactory
        .fromArgs("--avroTrustedPackages=com.spotify.scio.util.avrotrust.byinitializer")
        .as(classOf[AvroOptions])
      new AvroClassTrustInitializer().beforeProcessing(options)
      roundtrip(new InitializerPojo("a")).name shouldBe "a"
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
package com.spotify.scio.util.avrotrust.untrusted {
  class UntrustedPojo(var name: String) { def this() = this(null) }
}
