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

package com.spotify.scio

import java.lang.{Float => JFloat}

import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.values.{KV, TypeDescriptor}
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.util.ScioUtil

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[scio] object Implicits {

  // TODO: scala 2.11
  // private[scio] implicit class RichCoderRegistry(val r: CoderRegistry) extends AnyVal {
  private[scio] implicit class RichCoderRegistry(val r: CoderRegistry) {

    def registerScalaCoders(): Unit = {
      // Missing Coders from DataFlowJavaSDK
      r.registerCoder(classOf[JFloat], classOf[FloatCoder])

      r.registerCoder(classOf[Int], classOf[VarIntCoder])
      r.registerCoder(classOf[Long], classOf[VarLongCoder])
      r.registerCoder(classOf[Float], classOf[FloatCoder])
      r.registerCoder(classOf[Double], classOf[DoubleCoder])

      // Fall back to Kryo
      r.setFallbackCoderProvider(new CoderProvider {
        override def getCoder[T](`type`: TypeDescriptor[T]): Coder[T] = KryoAtomicCoder[T]
      })
    }

    def getScalaCoder[T: ClassTag]: Coder[T] = {
      val coder = try {
        // This may fail in come cases, i.e. Malformed class name in REPL
        // Always fall back to Kryo
        r.getDefaultCoder(TypeDescriptor.of(ScioUtil.classOf[T]))
      } catch {
        case _: Throwable => null
      }

      if (coder == null || coder.getClass == classOf[SerializableCoder[T]]) {
        KryoAtomicCoder[T]
      } else {
        coder
      }
    }

    def getScalaKvCoder[K: ClassTag, V: ClassTag]: Coder[KV[K, V]] = KvCoder.of(getScalaCoder[K], getScalaCoder[V])

  }

}
