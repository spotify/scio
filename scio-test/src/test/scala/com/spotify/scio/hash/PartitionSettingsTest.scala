/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.hash

import magnolify.guava.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionSettingsTest extends AnyFlatSpec with Matchers {

  def test[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long, fpp: Double, maxBytes: Int)(
    implicit hash: c.Hash[Long]
  ): Unit = {
    val actualInsertions = (expectedInsertions / 1.1).toLong // to prevent filter saturation
    val settings = c.partitionSettings(expectedInsertions, fpp, maxBytes)
    val filters = (0 until settings.partitions).map { i =>
      val part = Range.Long(i, actualInsertions, settings.partitions)
      c.create(part, settings.expectedInsertions, fpp)
    }

    all(filters.map(_.approxElementCount)) should be <= settings.expectedInsertions
    all(filters.map(_.expectedFpp)) should be <= fpp
    ()
  }

  def test[C <: ApproxFilterCompanion](c: C)(implicit hash: c.Hash[Long]): Unit = {
    val kb = 1024
    val mb = 1024 * 1024

    val filterName = c.getClass.getSimpleName.stripSuffix("$")

    filterName should "support partitions" in {
      test(c, 1L << 10, 0.01, 1 * kb)
      test(c, 1L << 10, 0.03, 1 * kb)
      test(c, 1L << 10, 0.05, 1 * kb)

      test(c, 1L << 20, 0.01, 1 * mb)
      test(c, 1L << 20, 0.03, 1 * mb)
      test(c, 1L << 20, 0.05, 1 * mb)
    }
  }

  test(BloomFilter)
  test(ABloomFilter)
}
