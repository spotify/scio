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

package com.spotify.scio.testing

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.CoderAssertions._
import org.apache.beam.sdk.coders.{AtomicCoder, StringUtf8Coder}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Foo(id: String)

class CoderAssertionsTest extends AnyFlatSpec with Matchers {
  // A coder which roundtrips incorrectly
  private def incorrectCoder: Coder[Foo] =
    Coder.beam(new AtomicCoder[Foo] {
      override def encode(value: Foo, outStream: OutputStream): Unit =
        StringUtf8Coder.of().encode(value.id, outStream)
      override def decode(inStream: InputStream): Foo =
        Foo(StringUtf8Coder.of().decode(inStream) + "wrongBytes")
    })

  "CoderAssertions" should "support roundtrip" in {
    Foo("bar") coderShould roundtrip()

    an[TestFailedException] should be thrownBy {
      implicit def coder: Coder[Foo] = incorrectCoder

      Foo("baz") coderShould roundtrip()
    }
  }

  it should "support fallback" in {
    val str = "boom"
    val cs: java.lang.CharSequence = str
    cs coderShould fallback()

    an[TestFailedException] should be thrownBy {
      str coderShould fallback()
    }
  }

  it should "support notFallback" in {
    val str = "boom"
    str coderShould notFallback()

    an[TestFailedException] should be thrownBy {
      val cs: java.lang.CharSequence = str
      cs coderShould notFallback()
    }
  }

  it should "support coderIsSerializable" in {
    coderIsSerializable[Foo]
    coderIsSerializable(Coder[Foo])

    // Inner class's Coder is not serializable
    case class InnerCaseClass(id: String)

    an[TestFailedException] should be thrownBy {
      coderIsSerializable[InnerCaseClass]
    }

    an[TestFailedException] should be thrownBy {
      coderIsSerializable(Coder[InnerCaseClass])
    }
  }
}
