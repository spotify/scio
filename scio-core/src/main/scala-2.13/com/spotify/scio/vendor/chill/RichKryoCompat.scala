/*
Copyright 2019 Twitter, Inc.

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

import scala.annotation.nowarn
import scala.collection.Factory
import scala.reflect._

trait RichKryoCompat { self: RichKryo =>

  def forTraversableSubclass[T, C <: Traversable[T]](
    @nowarn c: C with Traversable[T],
    isImmutable: Boolean = true
  )(implicit mf: ClassTag[C], f: Factory[T, C]): Kryo = {
    k.addDefaultSerializer(mf.runtimeClass, new TraversableSerializer(isImmutable)(f))
    k
  }

  def forTraversableClass[T, C <: Traversable[T]](
    @nowarn c: C with Traversable[T],
    isImmutable: Boolean = true
  )(implicit mf: ClassTag[C], f: Factory[T, C]): Kryo =
    forClass(new TraversableSerializer(isImmutable)(f))

  def forConcreteTraversableClass[T, C <: Traversable[T]](
    c: C with Traversable[T],
    isImmutable: Boolean = true
  )(implicit f: Factory[T, C]): Kryo = {
    // a ClassTag is not used here since its runtimeClass method does not return the concrete internal type
    // that Scala uses for small immutable maps (i.e., scala.collection.immutable.Map$Map1)
    k.register(c.getClass, new TraversableSerializer(isImmutable)(f))
    k
  }
}
