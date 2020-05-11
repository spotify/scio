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
import scala.util.{Failure, Try}

class GrpcSerializersTest extends AnyFlatSpec with Matchers {

  "StatusRuntimeException" should "roundtrip with all fields present" in {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of[String]("k", Metadata.ASCII_STRING_MARSHALLER), "v")

    roundtrip(
      Failure(
        new StatusRuntimeException(
          Status.OK.withCause(new RuntimeException("bar")).withDescription("bar"),
          metadata
        )
      )
    )
  }

  it should "roundtrip with all nullable fields absent" in {
    roundtrip(Failure(new StatusRuntimeException(Status.OK)))
  }

  private def roundtrip(t: Try[String]): Unit = {
    val kryoBCoder = CoderMaterializer.beamWithDefault(Coder[Try[String]])

    val bytes = CoderUtils.encodeToByteArray(kryoBCoder, t)
    val copy = CoderUtils.decodeFromByteArray(kryoBCoder, bytes)

    (t, copy) match {
      case (Failure(t1), Failure(t2)) =>
        (t1, t2) match {
          case (s1: StatusRuntimeException, s2: StatusRuntimeException) =>
            checkStatusEq(s1.getStatus, s2.getStatus)
            checkTrailersEq(s1.getTrailers, s2.getTrailers)
          case _ => fail(s"Exceptions ($t1, $t2) were not StatusRuntimeExceptions")
        }
      case _ => fail(s"Roundtrip value $copy was not equal to original value $t")
    }
  }

  private def checkTrailersEq(t1: Metadata, t2: Metadata): Unit =
    if (t1 != null && t2 != null) {
      t1.keys.size shouldEqual t2.keys.size
      t1.keys.asScala.foreach { k =>
        t1.get(Metadata.Key.of[String](k, Metadata.ASCII_STRING_MARSHALLER)) shouldEqual
          t2.get(Metadata.Key.of[String](k, Metadata.ASCII_STRING_MARSHALLER))
      }
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
