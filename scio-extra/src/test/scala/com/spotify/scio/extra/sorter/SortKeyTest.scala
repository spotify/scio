/*
 *   Copyright 2020 Spotify AB.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package com.spotify.scio.extra.sorter

import com.google.common.primitives.UnsignedBytes
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.extra.PropertySpec
import org.apache.beam.sdk.coders.{
  DoubleCoder,
  StringUtf8Coder,
  VarIntCoder,
  VarLongCoder,
  Coder => BCoder
}
import org.apache.beam.sdk.util.CoderUtils
import org.scalacheck._

class SortKeyTest extends PropertySpec {
  val byteOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    val byteComparator = UnsignedBytes.lexicographicalComparator;

    override def compare(x: Array[Byte], y: Array[Byte]): Int =
      byteComparator.compare(x, y)
  }

  property("StringSort") {
    forAll(Gen.listOfN[String](100, Gen.alphaStr)) { strings =>
      checkSort(strings)
    }
  }

  property("IntSort") {
    forAll(Gen.listOfN[Int](100, Gen.choose(-500, 500))) { ints =>
      checkSort(ints)
    }
  }

  property("LongSort") {
    forAll(Gen.listOfN[Long](100, Gen.choose(-500L, 500L))) { longs =>
      checkSort(longs)
    }
  }

  property("DoubleSort") {
    forAll(Gen.listOfN[Double](100, Gen.choose(-500.0, 500.0))) { doubles =>
      checkSort(doubles)
    }
  }

  // Check that Key's natural ordering matches byte array ordering
  def checkSort[Key: SortingKey: Ordering: Coder](items: Iterable[Key]): Unit = {
    val bCoder = CoderMaterializer.beamWithDefault(Coder[Key])

    items.toList.sorted
      .map(item => CoderUtils.encodeToByteArray(bCoder, item))
      .sliding(2)
      .forall { case Seq(i1, i2) => byteOrdering.lteq(i1, i2) }
  }
}
