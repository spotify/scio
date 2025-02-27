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

package com.spotify.scio.vendor.chill.config

import com.spotify.scio.vendor.chill._

import com.esotericsoftware.kryo.Kryo;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import scala.collection.JavaConverters._

/**
 * a builder for the ReflectingInstantiator use the copy(arg = value) to change values:
 * ReflectingInstantiatorBuilder() .copy(classes = List(classOf[Int])) .copy(skipMissing = true)
 * .build
 */
case class ReflectingInstantiatorBuilder(
  kryoClass: Class[_ <: Kryo] = classOf[Kryo],
  instantiatorStrategyClass: Class[_ <: InstantiatorStrategy] = classOf[StdInstantiatorStrategy],
  classes: Iterable[ClassRegistrar[_]] = Nil,
  serializers: Iterable[ReflectingRegistrar[_]] = Nil,
  defaults: Iterable[ReflectingDefaultRegistrar[_]] = Nil,
  registrationRequired: Boolean = false,
  skipMissing: Boolean = false
) {

  /**
   * These casts appear to be needed because scala's type system is able to express more carefully
   * than java, but these variance-free Iterables were defined in the Java code
   */
  def build: ReflectingInstantiator =
    new ReflectingInstantiator(
      kryoClass,
      instantiatorStrategyClass,
      classes.asJava,
      serializers.asJava,
      defaults.asJava,
      registrationRequired,
      skipMissing
    )
}
