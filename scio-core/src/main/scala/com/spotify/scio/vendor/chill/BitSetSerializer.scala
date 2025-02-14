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

import scala.collection.immutable.BitSet

class BitSetSerializer extends KSerializer[BitSet] {
  def write(k: Kryo, o: Output, v: BitSet): Unit = {
    val size = v.size
    o.writeInt(size, true)
    // Duplicates some data, but helps size on the other end:
    if (size > 0) {
      o.writeInt(v.max, true)
    }
    var previous: Int = -1
    v.foreach { vi =>
      if (previous >= 0) {
        o.writeInt(vi - previous, true)
      } else {
        o.writeInt(vi, true) // first item
      }
      previous = vi
    }
  }
  def read(k: Kryo, i: Input, c: Class[BitSet]): BitSet = {
    val size = i.readInt(true)
    if (size == 0) {
      BitSet.empty
    } else {
      var sum = 0
      val bits = new Array[Long](i.readInt(true) / 64 + 1)
      (0 until size).foreach { _ =>
        sum += i.readInt(true)
        bits(sum / 64) |= 1L << (sum % 64)
      }
      BitSet.fromBitMask(bits)
    }
  }
}
