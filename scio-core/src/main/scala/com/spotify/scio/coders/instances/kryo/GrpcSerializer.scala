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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.httpjson.HttpJsonStatusCode
import com.google.api.gax.rpc.{ApiException, ApiExceptionFactory}
import com.twitter.chill.KSerializer
import io.grpc.{Metadata, Status, StatusRuntimeException}

private[coders] class StatusSerializer extends KSerializer[Status] {
  override def write(kryo: Kryo, output: Output, status: Status): Unit = {
    output.writeInt(status.getCode.value())
    output.writeString(status.getDescription)
    kryo.writeClassAndObject(output, status.getCause)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Status]): Status = {
    val code = input.readInt()
    val description = input.readString()
    val cause = kryo.readClassAndObject(input).asInstanceOf[Throwable]

    Status
      .fromCodeValue(code)
      .withDescription(description)
      .withCause(cause)
  }
}

private[coders] class StatusRuntimeExceptionSerializer extends KSerializer[StatusRuntimeException] {
  private lazy val statusSer = new StatusSerializer()

  override def write(kryo: Kryo, output: Output, e: StatusRuntimeException): Unit = {
    kryo.writeObject(output, e.getStatus, statusSer)
    kryo.writeObjectOrNull(output, e.getTrailers, classOf[Metadata])
  }

  override def read(
    kryo: Kryo,
    input: Input,
    `type`: Class[StatusRuntimeException]
  ): StatusRuntimeException = {
    val status = kryo.readObject(input, classOf[Status], statusSer)
    val trailers = kryo.readObjectOrNull(input, classOf[Metadata])

    new StatusRuntimeException(status, trailers)
  }
}

private[coders] class GaxApiExceptionSerializer extends KSerializer[ApiException] {
  private lazy val statusSer = new StatusSerializer()
  override def write(kryo: Kryo, output: Output, e: ApiException): Unit = {
    kryo.writeObject(output, e.getMessage)
    kryo.writeClassAndObject(output, e.getCause)
    e.getStatusCode match {
      case grpc: GrpcStatusCode =>
        kryo.writeClass(output, classOf[GrpcStatusCode])
        kryo.writeObject(output, grpc.getTransportCode.toStatus, statusSer)
      case http: HttpJsonStatusCode =>
        kryo.writeClass(output, classOf[HttpJsonStatusCode])
        kryo.writeObject(output, http.getTransportCode)
      case statusCode =>
        kryo.writeClass(output, statusCode.getClass)
    }
    kryo.writeObject(output, e.isRetryable)
    // kryo.writeObjectOrNull(output, e.getErrorDetails, classOf[ErrorDetails])
  }

  override def read(
    kryo: Kryo,
    input: Input,
    `type`: Class[ApiException]
  ): ApiException = {
    val message = kryo.readObject(input, classOf[String])
    val cause = kryo.readClassAndObject(input).asInstanceOf[Throwable]
    val codeClass = kryo.readClass(input).getType
    val code = if (codeClass == classOf[GrpcStatusCode]) {
      val status = kryo.readObject(input, classOf[Status], statusSer)
      GrpcStatusCode.of(status.getCode)
    } else if (codeClass == classOf[HttpJsonStatusCode]) {
      val status = kryo.readObject(input, classOf[Integer])
      HttpJsonStatusCode.of(status)
    } else {
      null
    }
    val retryable = kryo.readObjectOrNull(input, classOf[Boolean])
    // val errorDetails = kryo.readObjectOrNull(input, classOf[ErrorDetails])

    ApiExceptionFactory.createException(
      message,
      cause,
      code,
      retryable
    )
  }
}
