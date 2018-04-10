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
import com.esotericsoftware.kryo.io.{Input, InputChunked, Output, OutputChunked}
import com.twitter.chill.KSerializer

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

/**
 * Based on [[org.apache.beam.sdk.coders.IterableLikeCoder]] and
 * [[org.apache.beam.sdk.util.BufferedElementCountingOutputStream]].
 */
private class JTraversableSerializer[T, C <: Traversable[T]](val bufferSize: Int = 64 * 1024)
(implicit cbf: CanBuildFrom[C, T, C])
  extends KSerializer[C] {

  override def write(kser: Kryo, out: Output, obj: C): Unit = {
    val i = obj.toIterator
    val chunked = new OutputChunked(out, bufferSize)
    while (i.hasNext) {
      chunked.writeBoolean(true)
      kser.writeClassAndObject(chunked, i.next())
    }
    chunked.writeBoolean(false)
    chunked.endChunks()
    chunked.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[C]): C = {
    val b = cbf()
    val chunked = new InputChunked(in, bufferSize)
    while (chunked.readBoolean()) {
      b += kser.readClassAndObject(chunked).asInstanceOf[T]
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
