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
import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder
import com.google.cloud.dataflow.sdk.values.{KV, TypeDescriptor}
import com.google.protobuf.Message
import com.spotify.scio.coders.{FloatCoder, KryoAtomicCoder}
import com.spotify.scio.util.ScioUtil
import org.apache.avro.specific.SpecificRecord

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[scio] object Implicits {

  private[scio] implicit class RichCoderRegistry(val r: CoderRegistry) extends AnyVal {

    def registerScalaCoders(): Unit = {
      // Missing Coders from DataFlowJavaSDK
      r.registerCoder(classOf[JFloat], classOf[FloatCoder])

      r.registerCoder(classOf[Int], classOf[VarIntCoder])
      r.registerCoder(classOf[Long], classOf[VarLongCoder])
      r.registerCoder(classOf[Float], classOf[FloatCoder])
      r.registerCoder(classOf[Double], classOf[DoubleCoder])

      // Fall back to Kryo
      r.setFallbackCoderProvider(new CoderProvider {
        override def getCoder[T](`type`: TypeDescriptor[T]): Coder[T] = {
          val cls = `type`.getRawType
          if (classOf[SpecificRecord] isAssignableFrom cls) {
            // TODO: what about GenericRecord?
            AvroCoder.of(cls).asInstanceOf[Coder[T]]
          } else if (classOf[Message] isAssignableFrom cls) {
            ProtoCoder.of(cls.asSubclass(classOf[Message])).asInstanceOf[Coder[T]]
          } else {
            KryoAtomicCoder[T]
          }
        }
      })
    }

    def getScalaCoder[T: ClassTag]: Coder[T] = {
      val coder = try {
        // This may fail in come cases, i.e. Malformed class name in REPL
        // Always fall back to Kryo
        r.getDefaultCoder(TypeDescriptor.of(ScioUtil.classOf[T]))
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
