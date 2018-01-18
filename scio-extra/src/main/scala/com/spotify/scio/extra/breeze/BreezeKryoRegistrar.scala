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

package com.spotify.scio.extra.breeze

import breeze.linalg.SparseVector
import breeze.storage.Zero
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.chill._

import scala.reflect.ClassTag

@KryoRegistrar
class BreezeKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.register(classOf[SparseVector[_]], new SparseVectorSerializer[Any])
  }
}

private class SparseVectorSerializer[T] extends KSerializer[SparseVector[T]] {
  override def read(kryo: Kryo, input: Input, tpe: Class[SparseVector[T]]): SparseVector[T] = {
    val zero = kryo.readClassAndObject(input).asInstanceOf[T]
    implicit val ct = ClassTag[T](zero.getClass)
    val len = input.read()
    val data = new Array[T](len)
    val index = new Array[Int](len)
    var i = 0
    while (i < len) {
      index(i) = input.read()
      data(i) = kryo.readClassAndObject(input).asInstanceOf[T]
      i += 1
    }
    new SparseVector[T](index, data, len)(Zero(zero))
  }

  override def write(kryo: Kryo, output: Output, vec: SparseVector[T]): Unit = {
    kryo.writeClassAndObject(output, vec.default)
    output.write(vec.activeSize)
    val it = vec.activeIterator
    while (it.hasNext) {
      val (n, v) = it.next()
      output.write(n)
      kryo.writeClassAndObject(output, v)
    }
  }
}
