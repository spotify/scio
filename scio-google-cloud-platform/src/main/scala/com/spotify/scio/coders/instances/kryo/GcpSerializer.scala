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

package com.spotify.scio.coders.instances.kryo

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException
import com.twitter.chill._

private[coders] class BigtableRetriesExhaustedExceptionSerializer
    extends KSerializer[BigtableRetriesExhaustedException] {

  private lazy val stringSerializer = new StringSerializer()
  private lazy val statusExceptionSerializer = new StatusRuntimeExceptionSerializer()

  override def write(kryo: Kryo, output: Output, e: BigtableRetriesExhaustedException): Unit = {
    kryo.writeObject(output, e.getMessage, stringSerializer)
    kryo.writeObject(output, e.getCause, statusExceptionSerializer)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    `type`: Class[BigtableRetriesExhaustedException]
  ): BigtableRetriesExhaustedException = {
    val message = kryo.readObject(input, classOf[String], stringSerializer)
    val cause = kryo.readObject(input, classOf[Throwable], statusExceptionSerializer)
    new BigtableRetriesExhaustedException(message, cause)
  }
}
