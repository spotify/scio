/*
 * Copyright 2016 Alex Chermenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.spotify.scio.vendor.chill.java

import TestCollections._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UnmodifiableCollectionsTest extends AnyWordSpec with Matchers {
  "UnmodifiableListSerializer" should {
    "UnmodifiableArrayList" in {
      val list = getUnmodifiableArrayList
      list should equal(serializeAndDeserialize(list))
    }

    "UnmodifiableLinkedList" in {
      val list = getUnmodifiableLinkedList
      list should equal(serializeAndDeserialize(list))
    }
  }

  "UnmodifiableMapSerializer" should {
    "UnmodifiableHashMap" in {
      val map = getUnmodifiableHashMap
      map should equal(serializeAndDeserialize(map))
    }
  }

  "UnmodifiableSortedMapSerializer" should {
    "UnmodifiableTreeMap" in {
      val map = getUnmodifiableTreeMap
      map should equal(serializeAndDeserialize(map))
    }
  }

  "UnmodifiableSetSerializer" should {
    "UnmodifiableHashSet" in {
      val set = getUnmodifiableHashSet
      set should equal(serializeAndDeserialize(set))
    }
  }

  "UnmodifiableSortedSetSerializer" should {
    "UnmodifiableTreeSet" in {
      val set = getUnmodifiableTreeSet
      set should equal(serializeAndDeserialize(set))
    }
  }

  "UnmodifiableCollectionSerializer" should {
    "UnmodifiableCollection" in {
      val before = getUnmodifiableCollection
      val after = serializeAndDeserialize(before)

      // UnmodifiableCollection has not own equals() method
      before.getClass should equal(after.getClass)
      Set(before.toArray: _*)
        .zip(Set(after.toArray: _*))
        .count(x => !x._1.equals(x._2)) should equal(0)
    }
  }
}
