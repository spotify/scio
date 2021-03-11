/*
 * Copyright 2021 Spotify AB
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

package com.spotify.scio.extra.sparkey.instances

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.beam.sdk

/** Used by SparkeyMap[K, V] and SparkeySet[T] to encode and decode with provided coders. */
object SparkeyCoderUtils {
  private[sparkey] def encode[T](value: T, coder: sdk.coders.Coder[T]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    coder.encode(value, output)
    output.toByteArray
  }

  private[sparkey] def decode[T](value: Array[Byte], coder: sdk.coders.Coder[T]): T =
    if (value == null) {
      null.asInstanceOf[T]
    } else {
      coder.decode(new ByteArrayInputStream(value))
    }
}
