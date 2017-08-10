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

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.Lists
import com.twitter.chill.KSerializer

import scala.collection.JavaConverters._

/**
 * Based on [[org.apache.beam.sdk.coders.IterableLikeCoder]] and
 * [[org.apache.beam.sdk.util.BufferedElementCountingOutputStream]].
 */
private class JIterableWrapperSerializer[T](val bufferSize: Int = 64 * 1024,
                                            val maxBufferSize: Int = 1024 * 1024)
  extends KSerializer[Iterable[T]] {

  override def write(kser: Kryo, out: Output, obj: Iterable[T]): Unit = {
    val buffer = new Output(bufferSize, maxBufferSize)
    var count = 0
    val i = obj.iterator
    while (i.hasNext) {
      val bufferPos = buffer.position()
      if (bufferPos >= bufferSize) {
        writeOutBuffer(buffer, bufferPos)
      }
      val toSer = i.next()
      var retry = true
      while (retry) {
        try {
          kser.writeClassAndObject(buffer, toSer)
          retry = false
          count += 1
        } catch {
          case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
            if (count == 0) {
              // buffer is empty but it's still not enough to serialize current object
              // just serialize it straight to the final output
              out.writeInt(1)
              kser.writeClassAndObject(out, toSer)
              retry = false
            } else {
              // write up to the position before failed serialization and retry with empty buffer
              writeOutBuffer(buffer, bufferPos)
            }
        }
      }
    }
    if (buffer.position() > 0) {
      out.writeInt(count)
      out.write(buffer.getBuffer, 0, buffer.position())
    }
    out.writeInt(0)

    def writeOutBuffer(buffer: Output, length: Int): Unit = {
      out.writeInt(count)
      out.write(buffer.getBuffer, 0, length)
      buffer.clear()
      count = 0
    }
  }

  override def read(kser: Kryo, in: Input, cls: Class[Iterable[T]]): Iterable[T] = {
    val list = Lists.newArrayList[T]
    var count = in.readInt()
    while (count > 0) {
      var i = 0
      while (i < count) {
        val item = kser.readClassAndObject(in).asInstanceOf[T]
        list.add(item)
        i += 1
      }
      count = in.readInt()
    }
    list.asInstanceOf[JIterable[T]].asScala
  }

}
