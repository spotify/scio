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

class ManifestSerializer[T] extends KSerializer[Manifest[T]] {
  val singletons: IndexedSeq[Manifest[_]] = IndexedSeq(
    Manifest.Any,
    Manifest.AnyVal,
    Manifest.Boolean,
    Manifest.Byte,
    Manifest.Char,
    Manifest.Double,
    Manifest.Float,
    Manifest.Int,
    Manifest.Long,
    Manifest.Nothing,
    Manifest.Null,
    Manifest.Object,
    Manifest.Short,
    Manifest.Unit
  )

  val singletonToIdx: Map[Manifest[_], Int] = singletons.zipWithIndex.toMap

  private def writeInternal(kser: Kryo, out: Output, obj: Manifest[_]): Unit = {
    val idxOpt = singletonToIdx.get(obj)
    if (idxOpt.isDefined) {
      // We offset by 1 to keep positive and save space
      out.writeInt(idxOpt.get + 1, true)
    } else {
      out.writeInt(0, true)
      kser.writeObject(out, obj.runtimeClass)
      // write the type arguments:
      val targs = obj.typeArguments
      out.writeInt(targs.size, true)
      out.flush
      targs.foreach(writeInternal(kser, out, _))
    }
  }

  def write(kser: Kryo, out: Output, obj: Manifest[T]): Unit = writeInternal(kser, out, obj)

  def read(kser: Kryo, in: Input, cls: Class[Manifest[T]]): Manifest[T] = {
    val sidx = in.readInt(true)
    if (sidx == 0) {
      val clazz = kser.readObject(in, classOf[Class[T]])
      val targsCnt = in.readInt(true)
      if (targsCnt == 0) {
        Manifest.classType(clazz)
      } else {
        // We don't need to know the cls:
        val typeArgs = (0 until targsCnt).map(_ => read(kser, in, null))
        Manifest.classType(clazz, typeArgs.head, typeArgs.tail: _*)
      }
    } else {
      singletons(sidx - 1).asInstanceOf[Manifest[T]]
    }
  }
}
