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

import java.lang.{Iterable => JIterable}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.Lists
import com.twitter.chill.KSerializer

import scala.collection.JavaConverters._

private class JIterableWrapperSerializer[T] extends KSerializer[Iterable[T]] {

  override def write(kser: Kryo, out: Output, obj: Iterable[T]): Unit = {
    val i = obj.iterator
    while (i.hasNext) {
      out.writeBoolean(true)
      kser.writeClassAndObject(out, i.next())
    }
    out.writeBoolean(false)
  }

  override def read(kser: Kryo, in: Input, cls: Class[Iterable[T]]): Iterable[T] = {
    val list = Lists.newArrayList[T]
    while (in.readBoolean()) {
      val item = kser.readClassAndObject(in).asInstanceOf[T]
      list.add(item)
    }
    list.asInstanceOf[JIterable[T]].asScala
  }

}
