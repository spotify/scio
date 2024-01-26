/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.coders.instance.kryo

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.InternalException
import com.google.cloud.bigtable.data.v2.models.MutateRowsException
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException
import com.spotify.scio.coders.instances.kryo.GrpcSerializerTest._
import io.grpc.Status.Code
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

object GcpSerializerTest {

  implicit val eqBigtableRetriesExhaustedException: Equality[BigtableRetriesExhaustedException] = {
    case (a: BigtableRetriesExhaustedException, b: BigtableRetriesExhaustedException) =>
      a.getMessage == b.getMessage &&
      ((Option(a.getCause), Option(b.getCause)) match {
        case (None, None) => true
        case (Some(ac: StatusRuntimeException), Some(bc: StatusRuntimeException)) =>
          eqStatusRuntimeException.areEqual(ac, bc)
        case _ =>
          false
      })
    case _ => false
  }

  implicit val eqMutateRowsException: Equality[MutateRowsException] = {
    case (a: MutateRowsException, b: MutateRowsException) =>
      // a.getCause == b.getCause &&
      a.getStatusCode == b.getStatusCode &&
      a.isRetryable == b.isRetryable &&
      a.getFailedMutations.size() == b.getFailedMutations.size() &&
      a.getFailedMutations.asScala.zip(b.getFailedMutations.asScala).forall { case (x, y) =>
        x.getIndex == y.getIndex &&
        eqGaxApiException.areEqual(x.getError, y.getError)
      }
    case _ =>
      false
  }

}

class GcpSerializerTest extends AnyFlatSpec with Matchers {

  import GcpSerializerTest._
  import com.spotify.scio.testing.CoderAssertions._

  "BigtableRetriesExhaustedException" should "roundtrip" in {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of[String]("k", Metadata.ASCII_STRING_MARSHALLER), "v")
    val cause = new StatusRuntimeException(
      Status.OK.withCause(new RuntimeException("bar")).withDescription("bar"),
      metadata
    )
    new BigtableRetriesExhaustedException("Error", cause) coderShould roundtrip()
  }

  "MutateRowsExceptionSerializer" should "roundtrip" in {
    val cause = new StatusRuntimeException(Status.OK)
    val apiException = new InternalException(cause, GrpcStatusCode.of(Code.OK), false)
    val failedMutations = List(MutateRowsException.FailedMutation.create(1, apiException))
    new MutateRowsException(cause, failedMutations.asJava, false) coderShould roundtrip()
  }

}
