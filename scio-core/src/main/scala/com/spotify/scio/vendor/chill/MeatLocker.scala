/*
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.scio.vendor.chill

import _root_.java.io.Serializable
object MeatLocker {
  def apply[T](t: T) = new MeatLocker(t)
}

/**
 * Use Kryo to provide a "box" which is efficiently Java serializable even if the underlying t is
 * not, as long as it is serializable with Kryo.
 *
 * Externalizer has replaced this class. Prefer that.
 */
class MeatLocker[T](@transient protected var t: T) extends Serializable {
  protected def pool: KryoPool = ScalaKryoInstantiator.defaultPool
  protected val tBytes: Array[Byte] = pool.toBytesWithClass(t)

  def get: T = {
    if (null == t) {
      // we were serialized
      t = copy
    }
    t
  }

  def copy: T = pool.fromBytes(tBytes).asInstanceOf[T]
}
