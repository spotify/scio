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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.twitter.chill.KSerializer

private class CoderSerializer[T](private val coder: Coder[T]) extends KSerializer[T] {

  override def write(kser: Kryo, out: Output, obj: T): Unit = {
    val bytes = CoderUtils.encodeToByteArray(coder, obj)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]): T =
    CoderUtils.decodeFromByteArray(coder, in.readBytes(in.readInt()))

}
