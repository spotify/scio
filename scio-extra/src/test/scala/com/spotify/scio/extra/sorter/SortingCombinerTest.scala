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

import com.spotify.scio.testing._

import scala.util.Random

class SortingCombinerTest extends PipelineSpec {
  import SortingCombinerTest._

  "inMemoryGroupSort" should "work on ungrouped values" in {
    runWithContext { sc =>
      val sorted = sc
        .parallelize(peopleData)
        .keyBy(_.country)
        .inMemoryGroupSort(_.age)

      sorted should containInAnyOrder(expected)
    }
  }

  it should "should contain all the items of the original collection" in {
    runWithContext { sc =>
      val sorted = sc
        .parallelize(peopleData)
        .keyBy(_.country)
        .inMemoryGroupSort(_.age)
        .flatMap(_._2)

      sorted should containInAnyOrder(peopleData)
    }
  }

  it should "work on already grouped values" in {
    runWithContext { sc =>
      val sorted = sc
        .parallelize(peopleData)
        .keyBy(_.country)
        .groupByKey
        .inMemoryGroupSort(_.age)

      sorted should containInAnyOrder(expected)
    }
  }
}

object SortingCombinerTest {
  case class Person(country: String, age: Int)

  val r = new Random()
  val countries = List("US", "CN", "MX")

  val peopleData = Random
    .shuffle((1 to 100).toList ++ (50 to 150).toList)
    .map(i => Person(countries(r.nextInt(3)), i % 25))

  val expected: Iterable[(String, Iterable[Person])] =
    peopleData.groupBy(_.country).map { case (country, people) =>
      (country, people.sortBy(_.age))
    }
}
