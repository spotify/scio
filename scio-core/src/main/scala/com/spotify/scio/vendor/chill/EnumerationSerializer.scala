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

import scala.collection.mutable.{Map => MMap}

class EnumerationSerializer extends KSerializer[Enumeration#Value] {
  private val enumMethod = "scala$Enumeration$$outerEnum"
  private val outerMethod = classOf[Enumeration#Value].getMethod(enumMethod)
  // Cache the enum lookup:
  private val enumMap = MMap[Enumeration#Value, Enumeration]()

  private def enumOf(v: Enumeration#Value): Enumeration =
    enumMap.synchronized {
      // TODO: hacky, but not clear how to fix:
      enumMap.getOrElseUpdate(
        v,
        outerMethod
          .invoke(v)
          .asInstanceOf[scala.Enumeration]
      )
    }

  def write(kser: Kryo, out: Output, obj: Enumeration#Value): Unit = {
    val enum = enumOf(obj)
    // Note due to the ObjectSerializer, this only really writes the class.
    kser.writeClassAndObject(out, enum)
    // Now, we just write the ID:
    out.writeInt(obj.id)
  }

  def read(kser: Kryo, in: Input, cls: Class[Enumeration#Value]): Enumeration#Value = {
    // Note due to the ObjectSerializer, this only really writes the class.
    val enum = kser.readClassAndObject(in).asInstanceOf[Enumeration]
    enum(in.readInt).asInstanceOf[Enumeration#Value]
  }
}
