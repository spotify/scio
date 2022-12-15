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

package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.specific.{SpecificData, SpecificFixed}
import org.apache.beam.sdk.coders.CustomCoder

import scala.reflect.{classTag, ClassTag}
/**
 * Implementation is legit only for SpecificFixed, not GenericFixed
 * @see
 *   [[org.apache.beam.sdk.coders.AvroCoder]]
 */
final private class SpecificFixedCoder[A <: SpecificFixed](cls: Class[A]) extends CustomCoder[A] {
  // lazy because AVRO Schema isn't serializable
  @transient private[this] lazy val schema: Schema = SpecificData.get().getSchema(cls)
  private[this] val size = SpecificData.get().getSchema(cls).getFixedSize

  def encode(value: A, outStream: OutputStream): Unit = {
    assert(value.bytes().length == size)
    outStream.write(value.bytes())
  }

  def decode(inStream: InputStream): A = {
    val bytes = new Array[Byte](size)
    inStream.read(bytes)
    val old = SpecificData.newInstance(cls, schema)
    SpecificData.get().createFixed(old, bytes, schema).asInstanceOf[A]
  }

  override def isRegisterByteSizeObserverCheap(value: A): Boolean = true

  override def getEncodedElementByteSize(value: A): Long = size.toLong

  override def consistentWithEquals(): Boolean = true

  override def structuralValue(value: A): AnyRef = value

  override def verifyDeterministic(): Unit = {}
}

private object SpecificFixedCoder {
  def apply[A <: SpecificFixed: ClassTag]: Coder[A] = {
    val cls = classTag[A].runtimeClass.asInstanceOf[Class[A]]
    Coder.beam(new SpecificFixedCoder[A](cls))
  }
}