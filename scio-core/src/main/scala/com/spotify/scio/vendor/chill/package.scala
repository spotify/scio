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

package com.spotify.scio.vendor

/** Scala extensions to the Kryo serialization library. */
package object chill {
  type Kryo = com.esotericsoftware.kryo.Kryo
  type KSerializer[T] = com.esotericsoftware.kryo.Serializer[T]
  type Input = com.esotericsoftware.kryo.io.Input
  type Output = com.esotericsoftware.kryo.io.Output

  implicit def toRich(k: Kryo): RichKryo = new RichKryo(k)
  implicit def toInstantiator(fn: Function0[Kryo]): KryoInstantiator = new KryoInstantiator {
    override def newKryo: Kryo = fn.apply
  }
  implicit def toRegistrar(fn: Function1[Kryo, Unit]): IKryoRegistrar = new IKryoRegistrar {
    def apply(k: Kryo): Unit = fn(k)
  }
  implicit def toRegistrar(items: Iterable[IKryoRegistrar]): IKryoRegistrar = new IKryoRegistrar {
    def apply(k: Kryo): Unit = items.foreach(_.apply(k))
  }
  def printIfRegistered(cls: Class[_]): IKryoRegistrar = new IKryoRegistrar {
    def apply(k: Kryo): Unit =
      if (k.alreadyRegistered(cls)) {
        System.err.printf("%s is already registered.", cls.getName)
      }
  }
  def assertNotRegistered(cls: Class[_]): IKryoRegistrar = new IKryoRegistrar {
    def apply(k: Kryo): Unit =
      assert(!k.alreadyRegistered(cls), String.format("%s is already registered.", cls.getName))
  }
}
