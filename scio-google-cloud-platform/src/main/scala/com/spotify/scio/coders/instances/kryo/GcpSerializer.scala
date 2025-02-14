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

import com.google.api.gax.rpc.{ApiException, StatusCode}
import com.google.cloud.bigtable.data.v2.models.MutateRowsException
import com.spotify.scio.vendor.chill._

private[coders] class MutateRowsExceptionSerializer extends KSerializer[MutateRowsException] {
  override def write(kryo: Kryo, output: Output, e: MutateRowsException): Unit = {
    kryo.writeClassAndObject(output, e.getCause)
    kryo.writeObject(output, e.getStatusCode.getCode)
    val failedMutations = e.getFailedMutations
    kryo.writeObject(output, failedMutations.size())
    failedMutations.forEach { fm =>
      kryo.writeObject(output, fm.getIndex)
      kryo.writeObject(output, fm.getError)
    }
    kryo.writeObject(output, e.isRetryable)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    `type`: Class[MutateRowsException]
  ): MutateRowsException = {
    val cause = kryo.readClassAndObject(input).asInstanceOf[Throwable]
    // generic status code. we lost transport information during serialization
    val code = kryo.readObject(input, classOf[StatusCode.Code])
    val statusCode = new StatusCode() {
      override def getCode: StatusCode.Code = code
      override def getTransportCode: AnyRef = null
    }
    val size = kryo.readObject(input, classOf[Integer])
    val failedMutations = new _root_.java.util.ArrayList[MutateRowsException.FailedMutation](size)
    (0 until size).foreach { _ =>
      val index = kryo.readObject(input, classOf[Integer])
      val error = kryo.readObject(input, classOf[ApiException])
      failedMutations.add(MutateRowsException.FailedMutation.create(index, error))
    }
    val retryable = kryo.readObject(input, classOf[Boolean])
    MutateRowsException.create(cause, statusCode, failedMutations, retryable)
  }
}
