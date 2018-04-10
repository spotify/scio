/*
 * Copyright 2018 Spotify AB.
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
import com.twitter.chill.KSerializer
import org.apache.beam.sdk.values.KV

private class KVSerializer[K, V] extends KSerializer[KV[K, V]] {

  override def write(kser: Kryo, out: Output, obj: KV[K, V]): Unit = {
    kser.writeClassAndObject(out, obj.getKey)
    kser.writeClassAndObject(out, obj.getValue)
  }

  override def read(kser: Kryo, in: Input, cls: Class[KV[K, V]]): KV[K, V] = {
    val k = kser.readClassAndObject(in).asInstanceOf[K]
    val v = kser.readClassAndObject(in).asInstanceOf[V]
    KV.of(k, v)
  }

}
