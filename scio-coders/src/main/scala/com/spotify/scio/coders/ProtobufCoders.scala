/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.coders

import scala.reflect.{ClassTag, classTag}

import com.google.protobuf._
import org.apache.beam.sdk.extensions.protobuf._

//
// Protobuf Coders
//
trait ProtobufCoders {

  implicit def bytestringCoder: Coder[ByteString] =
    Coder.beam(ByteStringCoder.of())

  implicit def protoMessageCoder[T <: Message : ClassTag]: Coder[T] =
    Coder.beam(ProtoCoder.of(classTag[T].runtimeClass.asInstanceOf[Class[T]]))
}
