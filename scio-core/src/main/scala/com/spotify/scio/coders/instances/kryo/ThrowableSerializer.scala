/*
 * Copyright 2024 Spotify AB
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

import com.esotericsoftware.kryo.KryoException
import com.google.api.gax.rpc.{ApiException, ApiExceptionFactory, StatusCode}
import com.spotify.scio.vendor.chill.{Input, KSerializer, Kryo, Output}
import io.grpc.{Status, StatusException, StatusRuntimeException}

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

/**
 * Java based serialization for `Throwable`. This uses replace/resolve for throwable that do not
 * respect the Serializable interface:
 *   - io.grpc.StatusException
 *   - io.grpc.StatusRuntimeException
 *   - com.google.api.gax.rpc.ApiException
 */
private object ThrowableSerializer {
  final private case class SerializableStatusException(
    code: Status.Code,
    desc: String,
    cause: Throwable
  )
  final private case class SerializableStatusRuntimeException(
    code: Status.Code,
    desc: String,
    cause: Throwable
  )
  final private case class SerializableApiException(
    message: String,
    code: StatusCode.Code,
    retryable: Boolean,
    cause: Throwable
  )

  final class ThrowableObjectOutputStream(out: OutputStream) extends ObjectOutputStream(out) {
    enableReplaceObject(true)
    override def replaceObject(obj: AnyRef): AnyRef = obj match {
      case e: StatusException =>
        SerializableStatusException(
          e.getStatus.getCode,
          e.getStatus.getDescription,
          e.getStatus.getCause
        )
      case e: StatusRuntimeException =>
        SerializableStatusRuntimeException(
          e.getStatus.getCode,
          e.getStatus.getDescription,
          e.getStatus.getCause
        )
      case e: ApiException =>
        SerializableApiException(e.getMessage, e.getStatusCode.getCode, e.isRetryable, e.getCause)
      case _ => obj
    }
  }

  final class ThrowableObjectInputStream(in: InputStream) extends ObjectInputStream(in) {
    enableResolveObject(true)
    override def resolveObject(obj: AnyRef): AnyRef = obj match {
      case SerializableStatusException(code, desc, cause) =>
        new StatusException(Status.fromCode(code).withDescription(desc).withCause(cause))
      case SerializableStatusRuntimeException(code, desc, cause) =>
        new StatusRuntimeException(Status.fromCode(code).withDescription(desc).withCause(cause))
      case SerializableApiException(message, code, retryable, cause) =>
        // generic status code. we lost transport information during serialization
        val c = new StatusCode() {
          override def getCode: StatusCode.Code = code
          override def getTransportCode: AnyRef = null
        }
        ApiExceptionFactory.createException(message, cause, c, retryable)
      case _ => obj
    }
  }
}

final private[coders] class ThrowableSerializer extends KSerializer[Throwable] {
  import ThrowableSerializer._
  override def write(kryo: Kryo, out: Output, obj: Throwable): Unit = {
    try {
      val objectStream = new ThrowableObjectOutputStream(out)
      objectStream.writeObject(obj)
      objectStream.flush()
    } catch {
      case e: Exception => throw new KryoException("Error during Java serialization.", e)
    }
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Throwable]): Throwable =
    try {
      val objectStream = new ThrowableObjectInputStream(input)
      objectStream.readObject.asInstanceOf[Throwable]
    } catch {
      case e: Exception => throw new KryoException("Error during Java deserialization.", e)
    }
}
