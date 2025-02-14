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

import scala.collection.immutable.SortedSet

class SortedSetSerializer[T] extends KSerializer[SortedSet[T]] {
  def write(kser: Kryo, out: Output, set: SortedSet[T]): Unit = {
    // Write the size
    out.writeInt(set.size, true)

    // Write the ordering
    kser.writeClassAndObject(out, set.ordering.asInstanceOf[AnyRef])
    set.foreach { t =>
      val tRef = t.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, tRef)
      // After each intermediate object, flush
      out.flush()
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[SortedSet[T]]): SortedSet[T] = {
    val size = in.readInt(true)
    val ordering = kser.readClassAndObject(in).asInstanceOf[Ordering[T]]

    // Go ahead and be faster, and not as functional cool, and be mutable in here
    var idx = 0
    val builder = SortedSet.newBuilder[T](ordering)
    builder.sizeHint(size)

    while (idx < size) {
      val item = kser.readClassAndObject(in).asInstanceOf[T]
      builder += item
      idx += 1
    }
    builder.result()
  }
}
