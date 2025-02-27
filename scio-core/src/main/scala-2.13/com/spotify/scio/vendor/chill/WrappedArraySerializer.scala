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

import scala.collection.mutable.{ArrayBuilder, ArraySeq}
import scala.reflect._

class WrappedArraySerializer[T] extends KSerializer[ArraySeq[T]] {
  def write(kser: Kryo, out: Output, obj: ArraySeq[T]): Unit = {
    // Write the class-manifest, we don't use writeClass because it
    // uses the registration system, and this class might not be registered
    kser.writeObject(out, obj.elemTag.runtimeClass)
    kser.writeClassAndObject(out, obj.array)
  }

  def read(kser: Kryo, in: Input, cls: Class[ArraySeq[T]]): ArraySeq[T] = {
    // Write the class-manifest, we don't use writeClass because it
    // uses the registration system, and this class might not be registered
    val clazz = kser.readObject(in, classOf[Class[T]])
    val array = kser.readClassAndObject(in).asInstanceOf[Array[T]]
    val bldr = ArrayBuilder.make[T](ClassTag[T](clazz))
    bldr.sizeHint(array.size)
    bldr ++= array
    bldr.result()
  }
}
