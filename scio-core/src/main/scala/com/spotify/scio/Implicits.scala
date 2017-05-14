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

import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.values.{KV, TypeDescriptor}

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[scio] object Implicits {

  private[scio] implicit class RichCoderRegistry(val r: CoderRegistry) extends AnyVal {

    def registerScalaCoders(): Unit = {
      // Missing Coders from DataFlowJavaSDK
      r.registerCoderForClass(classOf[JFloat], FloatCoder.of())

      r.registerCoderForClass(classOf[Int], VarIntCoder.of())
      r.registerCoderForClass(classOf[Long], VarLongCoder.of())
      r.registerCoderForClass(classOf[Float], FloatCoder.of())
      r.registerCoderForClass(classOf[Double], DoubleCoder.of())
    }

    def getScalaCoder[T: ClassTag]: Coder[T] = {
      val coder = try {
        // This may fail in come cases, i.e. Malformed class name in REPL
        // Always fall back to Kryo
        r.getCoder(ScioUtil.classOf[T])
      } catch {
        // Malformed class name is a `java.lang.InternalError` and cannot be caught by NonFatal
        case _: Throwable => null
      }

      if (coder == null || coder.getClass == classOf[SerializableCoder[T]]) {
        KryoAtomicCoder[T]
      } else {
        coder
      }
    }

    def getScalaKvCoder[K: ClassTag, V: ClassTag]: Coder[KV[K, V]] =
      KvCoder.of(getScalaCoder[K], getScalaCoder[V])

  }

}
