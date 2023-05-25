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

import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException
import com.spotify.scio.coders.instances.kryo.GrpcSerializerTest.eqStatusRuntimeException
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.apache.beam.sdk.util.CoderUtils
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

}

class GcpSerializerTest extends AnyFlatSpec with Matchers {

  import GcpSerializerTest._

  "GcpSerializer" should "roundtrip" in {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of[String]("k", Metadata.ASCII_STRING_MARSHALLER), "v")
    val cause = new StatusRuntimeException(
      Status.OK.withCause(new RuntimeException("bar")).withDescription("bar"),
      metadata
    )
    roundtrip(new BigtableRetriesExhaustedException("Error", cause))
  }

  private def roundtrip(t: BigtableRetriesExhaustedException): Unit = {
    val kryoBCoder = CoderMaterializer.beamWithDefault(Coder[BigtableRetriesExhaustedException])

    val bytes = CoderUtils.encodeToByteArray(kryoBCoder, t)
    val copy = CoderUtils.decodeFromByteArray(kryoBCoder, bytes)

    t shouldEqual copy
  }

}
