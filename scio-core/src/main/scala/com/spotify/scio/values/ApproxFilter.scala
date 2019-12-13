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

package com.spotify.scio.values
import java.io._

import com.google.common.io.{ByteStreams, CountingOutputStream}
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.AtomicCoder

/**
 * An [[ApproxFilter]] is an abstraction over various Approximate / Probabilistic
 * data structures used for checking membership of elements.
 *
 * This trait defines read-only immutable filters. The filters are primarily aimed
 * to be used as singleton [[SideInput]]s in Scio pipelines.
 *
 * For example usage see [[BloomFilter]]
 */
@experimental
trait ApproxFilter[T] extends Serializable {

  /**
   * Check if the filter may contain a given element.
   */
  def mightContain(t: T): Boolean

  /**
   * The serialized size of the filter in bytes.
   */
  def sizeInBytes: Int = {
    val cos = new CountingOutputStream(ByteStreams.nullOutputStream())
    new ObjectOutputStream(cos).writeObject(this)
    cos.getCount.toInt
  }

  // Helper for setting values when deserializing.
  // This is used by sub classes to easily set field
  protected def setField(name: String, value: Any): Unit = {
    val f = getClass.getDeclaredField(name)
    f.setAccessible(true)
    f.set(this, value)
  }
}

/**
 * [[Coder]]s for any class that implements the [[ApproxFilter]] trait.
 */
object ApproxFilter {

  /**
   * [[Coder]] for [[ApproxFilter]]
   */
  implicit def coder[T, AF[_] <: ApproxFilter[_]]: Coder[AF[T]] = {
    Coder.beam {
      new AtomicCoder[AF[T]] {
        override def encode(value: AF[T], outStream: OutputStream): Unit =
          new ObjectOutputStream(outStream).writeObject(value)
        override def decode(inStream: InputStream): AF[T] =
          new ObjectInputStream(inStream).readObject().asInstanceOf[AF[T]]
      }
    }
  }
}
