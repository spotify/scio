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
import com.google.api.gax.rpc.{ApiException, ApiExceptionFactory, StatusCode}
import io.grpc.{Status, StatusException, StatusRuntimeException}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object GrpcSerializerTest {

  val eqCause: Equality[Throwable] = {
    case (null, null)                 => true
    case (null, _)                    => false
    case (_, null)                    => false
    case (a: Throwable, b: Throwable) =>
      a.getClass == b.getClass &&
      a.getMessage == b.getMessage
    case _ => false
  }

  private val eqStatus: Equality[Status] = {
    case (a: Status, b: Status) =>
      a.getCode == b.getCode &&
      a.getDescription == b.getDescription &&
      eqCause.areEqual(a.getCause, b.getCause)
    case _ => false
  }

  implicit val eqStatusException: Equality[StatusException] = {
    case (a: StatusException, b: StatusException) =>
      // skip trailers check
      eqStatus.areEqual(a.getStatus, b.getStatus)
    case _ => false
  }

  implicit val eqStatusRuntimeException: Equality[StatusRuntimeException] = {
    case (a: StatusRuntimeException, b: StatusRuntimeException) =>
      // skip trailers check
      eqStatus.areEqual(a.getStatus, b.getStatus)
    case _ => false
  }

  val eqStatusCode: Equality[StatusCode] = {
    case (a: StatusCode, b: StatusCode) =>
      a.getCode == b.getCode
    case _ => false
  }

  implicit val eqGaxApiException: Equality[ApiException] = {
    case (a: ApiException, b: ApiException) =>
      eqCause.areEqual(a.getCause, b.getCause) &&
      a.getMessage == b.getMessage &&
      eqStatusCode.areEqual(a.getStatusCode, b.getStatusCode) &&
      a.isRetryable == b.isRetryable
    case _ => false
  }

  final case class RootException[T <: Throwable](message: String, cause: T)

  implicit def eqRootException[T <: Throwable](implicit
    eq: Equality[T]
  ): Equality[RootException[T]] = {
    case (a: RootException[T], b: RootException[T]) =>
      a.message == b.message && eq.areEqual(a.cause, b.cause)
    case _ => false
  }
}

class GrpcSerializerTest extends AnyFlatSpec with Matchers {

  import GrpcSerializerTest._
  import com.spotify.scio.testing.CoderAssertions._

  "StatusRuntime" should "roundtrip" in {
    val statusException = new StatusException(
      Status.OK.withCause(new RuntimeException("bar")).withDescription("baz")
    )
    statusException coderShould roundtrip()
    RootException("root", statusException) coderShould roundtrip()
  }

  "StatusRuntimeException" should "roundtrip" in {
    val statusRuntimeException = new StatusRuntimeException(
      Status.OK.withCause(new RuntimeException("bar")).withDescription("baz")
    )
    statusRuntimeException coderShould roundtrip()
    RootException("root", statusRuntimeException) coderShould roundtrip()
  }

  "Gax API exception" should "roundtrip" in {
    val apiException = ApiExceptionFactory.createException(
      "foo",
      new RuntimeException("bar"),
      GrpcStatusCode.of(Status.NOT_FOUND.getCode),
      false
    )
    apiException coderShould roundtrip()
    RootException("root", apiException) coderShould roundtrip()
  }
}
