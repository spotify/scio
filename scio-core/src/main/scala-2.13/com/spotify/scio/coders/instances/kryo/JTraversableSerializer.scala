/*
 * Copyright 2019 Spotify AB.
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
import com.esotericsoftware.kryo.io.{Input, InputChunked, Output, OutputChunked}
import com.twitter.chill.KSerializer

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.compat._

/**
 * Based on [[org.apache.beam.sdk.coders.IterableLikeCoder]] and
 * [[org.apache.beam.sdk.util.BufferedElementCountingOutputStream]].
 */
private[coders] class JTraversableSerializer[T, C <: Traversable[T]](
  val bufferSize: Int = 64 * 1024
)(implicit cbf: Factory[T, C])
    extends KSerializer[C] {
  override def write(kser: Kryo, out: Output, obj: C): Unit = {
    val i = obj.iterator
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
    val b = cbf.newBuilder
    val chunked = new InputChunked(in, bufferSize)
    while (chunked.readBoolean()) {
      b += kser.readClassAndObject(chunked).asInstanceOf[T]
    }
    b.result()
  }
}

// workaround for Java Iterable/Collection missing proper equality check
abstract private[coders] class JWrapperCBF[T] extends Factory[T, Iterable[T]] {
  def asScala(xs: java.util.List[T]): Iterable[T]

  class JIterableWrapperBuilder extends mutable.Builder[T, Iterable[T]] {
    private val xs = new java.util.ArrayList[T]()

    override def addOne(elem: T): this.type = {
      xs.add(elem)
      this
    }

    override def clear(): Unit = xs.clear()
    override def result(): Iterable[T] = asScala(xs)
  }

  override def fromSpecific(it: IterableOnce[T]): Iterable[T] = {
    val b = new JIterableWrapperBuilder
    it.foreach(b += _)
    b.result()
  }

  override def newBuilder: mutable.Builder[T, Iterable[T]] = new JIterableWrapperBuilder
}

private[coders] class JIterableWrapperCBF[T] extends JWrapperCBF[T] {
  override def asScala(xs: java.util.List[T]): Iterable[T] =
    xs.asInstanceOf[java.lang.Iterable[T]].asScala
}

private[coders] class JCollectionWrapperCBF[T] extends JWrapperCBF[T] {
  override def asScala(xs: java.util.List[T]): Iterable[T] =
    xs.asInstanceOf[java.util.Collection[T]].asScala
}
