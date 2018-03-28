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
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.instantiator.sun.SunReflectionFactoryInstantiator

import scala.reflect.ClassTag

class CaseClassSerializer[T: ClassTag](kryo: Kryo) extends FieldSerializer[T](kryo, classOf[T]) {

  val objectCreator: ObjectInstantiator[T] = new SunReflectionFactoryInstantiator[T](classOf[T])

  override def create(kryo: Kryo, input: Input, classType: Class[T]): T = {
    objectCreator.newInstance()
  }

}
