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
import com.google.api.gax.rpc.ApiException
import com.google.cloud.bigtable.data.v2.models.MutateRowsException
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

private[coders] class MutateRowsExceptionSerializer extends KSerializer[MutateRowsException] {
  private lazy val apiExceptionSer = new GaxApiExceptionSerializer()
  override def write(kryo: Kryo, output: Output, e: MutateRowsException): Unit = {
    kryo.writeClassAndObject(output, e.getCause)
    val failedMutations = e.getFailedMutations
    kryo.writeObject(output, failedMutations.size())
    failedMutations.forEach { fm =>
      kryo.writeObject(output, fm.getIndex)
      kryo.writeObject(output, fm.getError, apiExceptionSer)
    }
    kryo.writeObject(output, e.isRetryable)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    `type`: Class[MutateRowsException]
  ): MutateRowsException = {
    val cause = kryo.readClassAndObject(input).asInstanceOf[Throwable]
    val size = kryo.readObject(input, classOf[Integer])
    val failedMutations = new _root_.java.util.ArrayList[MutateRowsException.FailedMutation](size)
    (0 until size).foreach { _ =>
      val index = kryo.readObject(input, classOf[Integer])
      val error = kryo.readObject(input, classOf[ApiException], apiExceptionSer)
      failedMutations.add(MutateRowsException.FailedMutation.create(index, error))
    }
    val retryable = kryo.readObject(input, classOf[Boolean])
    new MutateRowsException(cause, failedMutations, retryable)
  }
}
