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

import scala.collection.immutable.{ArraySeq, Range}

/**
 * Scala collections registrar for compatibility between 2.12- and 2.13+.
 *
 * For 2.12- there's no extra classes that need to be registered.
 * @see
 *   [[ScalaCollectionsRegistrar]] and [[AllScalaRegistrar]] for all the provided registrations.
 */
private[chill] class AllScalaRegistrarCompat_0_9_5 extends IKryoRegistrar {
  override def apply(newK: Kryo): Unit =
    newK.register(classOf[Range.Exclusive])

}

private[chill] class AllScalaRegistrarCompat extends IKryoRegistrar {
  override def apply(newK: Kryo): Unit = {
    // creating actual BigVector is too heavy, just register the class by name
    val t: TraversableSerializer[Any, Vector[_]] = new TraversableSerializer(true)
    newK.register(Class.forName("scala.collection.immutable.Vector0$"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector1"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector2"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector3"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector4"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector5"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector6"), t)
    newK.registerClasses(
      Seq(
        ArraySeq.unsafeWrapArray(Array[Byte]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Short]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Int]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Long]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Float]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Double]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Boolean]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Char]()).getClass,
        ArraySeq.unsafeWrapArray(Array[String]()).getClass
      )
    )
  }
}
