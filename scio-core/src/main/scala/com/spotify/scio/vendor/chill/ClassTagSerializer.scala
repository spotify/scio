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

import scala.reflect.ClassTag

class ClassTagSerializer[T] extends KSerializer[ClassTag[T]] {
  def write(kser: Kryo, out: Output, obj: ClassTag[T]): Unit =
    kser.writeObject(out, obj.runtimeClass)

  def read(kser: Kryo, in: Input, cls: Class[ClassTag[T]]): ClassTag[T] = {
    val clazz = kser.readObject(in, classOf[Class[T]])
    ClassTag(clazz)
  }
}
