/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill

import com.esotericsoftware.kryo.io.ByteBufferInputStream

import _root_.java.io.{InputStream, Serializable}
import _root_.java.nio.ByteBuffer

import scala.util.control.Exception.allCatch

import scala.reflect._

/**
 * Enrichment pattern to add methods to Kryo objects TODO: make this a value-class in scala 2.10
 * This also follows the builder pattern to allow easily chaining this calls
 */
class RichKryo(val k: Kryo) extends RichKryoCompat {
  def alreadyRegistered(klass: Class[_]): Boolean =
    k.getClassResolver.getRegistration(klass) != null

  def alreadyRegistered[T](implicit cmf: ClassTag[T]): Boolean = alreadyRegistered(cmf.runtimeClass)

  def forSubclass[T](kser: KSerializer[T])(implicit cmf: ClassTag[T]): Kryo = {
    k.addDefaultSerializer(cmf.runtimeClass, kser)
    k
  }

  def forClass[T](kser: KSerializer[T])(implicit cmf: ClassTag[T]): Kryo = {
    k.register(cmf.runtimeClass, kser)
    k
  }

  /**
   * Use Java serialization, which is very slow. avoid this if possible, but for very rare classes
   * it is probably fine
   */
  def javaForClass[T <: Serializable](implicit cmf: ClassTag[T]): Kryo = {
    k.register(cmf.runtimeClass, new com.esotericsoftware.kryo.serializers.JavaSerializer)
    k
  }

  /**
   * Use Java serialization, which is very slow. avoid this if possible, but for very rare classes
   * it is probably fine
   */
  def javaForSubclass[T <: Serializable](implicit cmf: ClassTag[T]): Kryo = {
    k.addDefaultSerializer(
      cmf.runtimeClass,
      new com.esotericsoftware.kryo.serializers.JavaSerializer
    )
    k
  }

  def registerClasses(klasses: TraversableOnce[Class[_]]): Kryo = {
    klasses.foreach { klass: Class[_] =>
      if (!alreadyRegistered(ClassTag(klass)))
        k.register(klass)
    }
    k
  }

  /** Populate the wrapped Kryo instance with this registrar */
  def populateFrom(reg: IKryoRegistrar): Kryo = {
    reg(k)
    k
  }

  def fromInputStream(s: InputStream): Option[AnyRef] = {
    // Can't reuse Input and call Input#setInputStream everytime
    val streamInput = new Input(s)
    allCatch.opt(k.readClassAndObject(streamInput))
  }

  def fromByteBuffer(b: ByteBuffer): Option[AnyRef] =
    fromInputStream(new ByteBufferInputStream(b))
}
