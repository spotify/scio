/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.coders.instances.kryo

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class GrpcSerializersTest extends AnyFlatSpec with Matchers {

  "StatusRuntimeException" should "roundtrip with nullable fields present" in {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of[String]("k", Metadata.ASCII_STRING_MARSHALLER), "v")

    roundtrip(
      new StatusRuntimeException(
        Status.OK.withCause(new RuntimeException("bar")).withDescription("bar"),
        metadata
      )
    )
  }

  it should "roundtrip with nullable fields absent" in {
    roundtrip(new StatusRuntimeException(Status.OK))
  }

  private def roundtrip(t: StatusRuntimeException): Unit = {
    val kryoBCoder = CoderMaterializer.beamWithDefault(Coder[StatusRuntimeException])

    val bytes = CoderUtils.encodeToByteArray(kryoBCoder, t)
    val copy = CoderUtils.decodeFromByteArray(kryoBCoder, bytes)

    checkStatusEq(t.getStatus, copy.getStatus)
    checkTrailersEq(t.getTrailers, copy.getTrailers)
  }

  private def checkTrailersEq(metadata1: Metadata, metadata2: Metadata): Unit =
    (Option(metadata1), Option(metadata2)) match {
      case (Some(m1), Some(m2)) =>
        m1.keys.size shouldEqual m2.keys.size
        m1.keys.asScala.foreach { k =>
          m1.get(Metadata.Key.of[String](k, Metadata.ASCII_STRING_MARSHALLER)) shouldEqual
            m2.get(Metadata.Key.of[String](k, Metadata.ASCII_STRING_MARSHALLER))
        }
      case (None, None) =>
      case _            => fail(s"Metadata were unequal: ($metadata1, $metadata2)")
    }

  private def checkStatusEq(s1: Status, s2: Status): Unit = {
    s1.getCode shouldEqual s2.getCode
    s1.getDescription shouldEqual s2.getDescription
    if (s1.getCause != null) {
      s1.getCause.getClass shouldEqual s2.getCause.getClass
      s1.getCause.getMessage shouldEqual s2.getCause.getMessage
    } else if (s2.getCause != null) {
      fail(s"Status $s1 is missing a cause")
    }
  }
}
