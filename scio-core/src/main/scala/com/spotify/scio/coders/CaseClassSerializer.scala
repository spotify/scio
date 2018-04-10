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
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.spotify.scio.util.ScioUtil
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.instantiator.sun.SunReflectionFactoryInstantiator

import scala.reflect.ClassTag

/**
  * Optimized serializer that can be used for case classes.
  * Has lower memory and CPU overhead than default serialization.
  * Sample usage:
  *
  * {{{
  * import com.spotify.scio._
  * import com.spotify.scio.coders.{CaseClassSerializer, KryoRegistrar}
  * import com.spotify.scio.examples.common.ExampleData
  * import com.twitter.chill.{IKryoRegistrar, Kryo}
  *
  * object WordCount {
  *
  * @KryoRegistrar
  * class OptimizedKryoRegistrar extends IKryoRegistrar {
  *     override def apply(k: Kryo): Unit = {
  *       k.addDefaultSerializer(classOf[Tuple2[_,_]],  new CaseClassSerializer[Tuple2[_,_]](k))
  *     }
  *   }
  *
  * def main(cmdlineArgs: Array[String]): Unit = {
  *     val (sc, args) = ContextAndArgs(cmdlineArgs)
  *     val input = args.getOrElse("input", ExampleData.KING_LEAR)
  *     val output = args("output")
  *     sc.textFile(input)
  *       .map { w =>
  *         val trimmed = w.trim
  *         trimmed
  *       }
  *       .filter { w => w.nonEmpty}
  *       .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
  *       .countByValue
  *       .map(t => t._1 + ": " + t._2)
  *       .saveAsTextFile(output)
  *     sc.close().waitUntilFinish()
  * }
  *
  * }
  * }}}
  *
  * @param kryo Kryo instance to be used for underlying serialization
  * @tparam T Underlying serialized type
  */
class CaseClassSerializer[T: ClassTag](kryo: Kryo)
  extends FieldSerializer[T](kryo, ScioUtil.classOf[T]) {

  private val objectCreator: ObjectInstantiator[T] =
    new SunReflectionFactoryInstantiator[T](ScioUtil.classOf[T])

  override def create(kryo: Kryo, input: Input, classType: Class[T]): T = {
    objectCreator.newInstance()
  }

}
