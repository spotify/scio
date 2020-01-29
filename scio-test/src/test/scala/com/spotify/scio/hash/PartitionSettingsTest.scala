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

  def test(expectedInsertions: Long, fpp: Double, maxBytes: Int): Unit = {
    val settings = BloomFilter.partitionSettings(expectedInsertions, fpp, maxBytes)
    val filters = (0 until settings.partitions).map { i =>
      val part = Range.Long(i, expectedInsertions, settings.partitions)
      BloomFilter.create(part, settings.expectedInsertions, fpp)
    }
    println(s"test($expectedInsertions, $fpp) = $settings")
    filters.foreach { f =>
      println(f.approxElementCount, f.expectedFpp)
    }
    all(filters.map(_.approxElementCount)) should be <= settings.expectedInsertions
    all(filters.map(_.expectedFpp)) should be <= fpp
  }

  val kb = 1024
  val mb = 1024 * 1024

  "BloomFilter" should "support partition settings" in {
    test(1L << 10, 0.01, 1 * kb)
    test(1L << 10, 0.03, 1 * kb)
    test(1L << 10, 0.05, 1 * kb)

    test(1L << 20, 0.01, 1 * mb)
    test(1L << 20, 0.03, 1 * mb)
    test(1L << 20, 0.05, 1 * mb)
  }
}
