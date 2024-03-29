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

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.{ApiException, ApiExceptionFactory}
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

object GrpcSerializerTest {
  private val eqMetadata: Equality[Metadata] = {
    case (a: Metadata, b: Metadata) =>
      a.keys().size() == b.keys().size() &&
      a.keys.asScala.forall { k =>
        val strKey = Metadata.Key.of[String](k, Metadata.ASCII_STRING_MARSHALLER)
        a.get(strKey) == b.get(strKey)
      }
    case _ => false
  }

  private val eqStatus: Equality[Status] = {
    case (a: Status, b: Status) =>
      a.getCode == b.getCode &&
      a.getDescription == b.getDescription &&
      ((Option(a.getCause), Option(b.getCause)) match {
        case (None, None) =>
          true
        case (Some(ac), Some(bc)) =>
          ac.getClass == bc.getClass &&
          ac.getMessage == bc.getMessage
        case _ =>
          false
      })
    case _ => false
  }

  implicit val eqStatusRuntimeException: Equality[StatusRuntimeException] = {
    case (a: StatusRuntimeException, b: StatusRuntimeException) =>
      a.getMessage == b.getMessage &&
      eqStatus.areEqual(a.getStatus, b.getStatus) &&
      ((Option(a.getTrailers), Option(b.getTrailers)) match {
        case (None, None)         => true
        case (Some(am), Some(bm)) => eqMetadata.areEqual(am, bm)
        case _                    => false
      })
    case _ => false
  }

  implicit val eqGaxApiException: Equality[ApiException] = {
    case (a: ApiException, b: ApiException) =>
      // a.getCause == b.getCause &&
      a.getMessage == b.getMessage &&
      a.getStatusCode == b.getStatusCode
      a.isRetryable == b.isRetryable
    case _ => false
  }

}

class GrpcSerializerTest extends AnyFlatSpec with Matchers {

  import GrpcSerializerTest._
  import com.spotify.scio.testing.CoderAssertions._

  "StatusRuntimeException" should "roundtrip with nullable fields present" in {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of[String]("k", Metadata.ASCII_STRING_MARSHALLER), "v")
    val statusRuntimeException = new StatusRuntimeException(
      Status.OK.withCause(new RuntimeException("bar")).withDescription("bar"),
      metadata
    )

    statusRuntimeException coderShould roundtrip()
  }

  it should "roundtrip with nullable fields absent" in {
    val statusRuntimeException = new StatusRuntimeException(Status.OK)
    statusRuntimeException coderShould roundtrip()
  }

  "Gax API exception" should "roundtrip" in {
    val cause = new StatusRuntimeException(Status.NOT_FOUND)
    ApiExceptionFactory.createException(
      cause,
      GrpcStatusCode.of(Status.NOT_FOUND.getCode),
      false
    ) coderShould roundtrip()
  }
}
