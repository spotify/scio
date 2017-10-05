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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.twitter.chill.KSerializer

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

/**
 * Based on [[org.apache.beam.sdk.coders.IterableLikeCoder]] and
 * [[org.apache.beam.sdk.util.BufferedElementCountingOutputStream]].
 */
private class JTraversableSerializer[T, C <: Traversable[T]]
(val bufferSize: Int = 64 * 1024, val maxBufferSize: Int = 1024 * 1024)
(implicit cbf: CanBuildFrom[C, T, C])
  extends KSerializer[C] {

  override def write(kser: Kryo, out: Output, obj: C): Unit = {
    val buffer = new Output(bufferSize, maxBufferSize)
    var count = 0
    obj.foreach { elem =>
      val bufferPos = buffer.position()
      var retry = true
      while (retry) {
        try {
          kser.writeClassAndObject(buffer, elem)
          retry = false
          count += 1
        } catch {
          case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
            if (count == 0) {
              // buffer was empty but it still wasn't enough to serialize current object
              // just serialize it straight to the final output
              out.writeInt(1)
              kser.writeClassAndObject(out, elem)
              buffer.clear() // clear possibly corrupted buffer
              retry = false
            } else {
              // write up to the position before failed serialization and retry with empty buffer
              writeOutBuffer(buffer, bufferPos)
            }
        }
      }
    }
    if (count > 0) {
      writeOutBuffer(buffer, buffer.position())
    }
    out.writeInt(0)

    def writeOutBuffer(buffer: Output, length: Int): Unit = {
      assume(count > 0, "Count can't be zero")
      out.writeInt(count)
      out.write(buffer.getBuffer, 0, length)
      buffer.clear()
      count = 0
    }
  }

  override def read(kser: Kryo, in: Input, cls: Class[C]): C = {
    val b = cbf()
    var count = in.readInt()
    while (count > 0) {
      var i = 0
      while (i < count) {
        val item = kser.readClassAndObject(in).asInstanceOf[T]
        b += item
        i += 1
      }
      count = in.readInt()
    }
    b.result()
  }

}

// workaround for Java Iterable/Collection missing proper equality check
private abstract class JWrapperCBF[T] extends CanBuildFrom[Iterable[T], T, Iterable[T]] {
  override def apply(from: Iterable[T]): mutable.Builder[T, Iterable[T]] = {
    val b = new JIterableWrapperBuilder
    from.foreach(b += _)
    b
  }
  override def apply(): mutable.Builder[T, Iterable[T]] = new JIterableWrapperBuilder
  def asScala(xs: java.util.List[T]): Iterable[T]

  class JIterableWrapperBuilder extends mutable.Builder[T, Iterable[T]] {
    private val xs = new java.util.ArrayList[T]()
    // scalastyle:off method.name
    override def +=(elem: T): this.type = {
      xs.add(elem)
      this
    }
    // scalastyle:on method.name
    override def clear(): Unit = xs.clear()
    override def result(): Iterable[T] = asScala(xs)
  }
}

private class JIterableWrapperCBF[T] extends JWrapperCBF[T] {
  override def asScala(xs: java.util.List[T]): Iterable[T] =
    xs.asInstanceOf[java.lang.Iterable[T]].asScala
}

private class JCollectionWrapperCBF[T] extends JWrapperCBF[T] {
  override def asScala(xs: java.util.List[T]): Iterable[T] =
    xs.asInstanceOf[java.util.Collection[T]].asScala
}
