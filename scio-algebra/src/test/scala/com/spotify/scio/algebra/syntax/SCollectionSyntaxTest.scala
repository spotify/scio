/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.algebra.syntax

import com.spotify.scio.testing._

import scala.util.Try

class SCollectionSyntaxTest extends PipelineSpec {

  "CatsSyntax" should "support tupleLeft" in {
    runWithContext { sc =>
      val data = Seq(Option(1))
      val expected = Seq(Option((2, 1)))

      val result = sc.parallelize(data).tupleLeft(2)

      result should containInAnyOrder(expected)
    }
  }

  it should "support tupleRight" in {
    runWithContext { sc =>
      val data = Seq(Option(1))
      val expected = Seq(Option((1, 2)))

      val result = sc.parallelize(data).tupleRight(2)

      result should containInAnyOrder(expected)
    }
  }

  it should "support as" in {
    runWithContext { sc =>
      val data = Seq(List(1, 2, 3))
      val expected = Seq(List("foobar", "foobar", "foobar"))

      val result = sc.parallelize(data).as("foobar")

      result should containInAnyOrder(expected)
    }
  }

  it should "support mapF" in {
    runWithContext { sc =>
      val data = Seq(Option(1))
      val expected = Seq(Option(2))

      val result = sc.parallelize(data).mapF(_ + 1)
      result should containInAnyOrder(expected)

      val listData = Seq(List(1, 2, 3), List(4, 5, 6))
      val expectedList = Seq(List(2, 3, 4), List(5, 6, 7))

      val resultList = sc.parallelize(listData).mapF(_ + 1)
      resultList should containInAnyOrder(expectedList)
    }
  }

  it should "support flatMapF" in {
    runWithContext { sc =>
      val m: Map[Int, String] = Map(1 -> "one", 3 -> "three")
      val data = Seq(Option(1), Option(2), None)
      val expected = Seq(Some("one"), None, None)

      val result = sc.parallelize(data).flatMapF(m.get)
      result should containInAnyOrder(expected)
    }
  }

  it should "support mapFilterF" in {
    runWithContext { sc =>
      val m: Map[Int, String] = Map(1 -> "one", 3 -> "three")
      val data = Seq(Option(1), Option(2), None)
      val expected = Seq(Some("one"), None, None)

      val result = sc.parallelize(data).mapFilterF(m.get)
      result should containInAnyOrder(expected)

      val listData = Seq(List(1, 2, 3), List(4, 5, 6))
      val expectedList = Seq(List("one", "three"), List())

      val resultList = sc.parallelize(listData).mapFilterF(m.get)
      resultList should containInAnyOrder(expectedList)
    }
  }

  it should "support filterF" in {
    runWithContext { sc =>
      val data = Seq(Option(1), Option(2), None)
      val expected = Seq(Option(1), None, None)

      val result = sc.parallelize(data).filterF(_ <= 1)
      result should containInAnyOrder(expected)

      val listData = Seq(List(1, 2, 3), List(4, 5, 6))
      val expectedList = Seq(List(1), List())

      val resultList = sc.parallelize(listData).filterF(_ <= 1)
      resultList should containInAnyOrder(expectedList)
    }
  }

  it should "support nonEmptyF" in {
    runWithContext { sc =>
      val data = Seq(Option(1), Option(2), None)
      val expected = Seq(Option(1))

      val result = sc.parallelize(data).nonEmptyF(_ <= 1)
      result should containInAnyOrder(expected)
    }
  }

  it should "support emptyF" in {
    runWithContext { sc =>
      val data = Seq(List(1, 2, 3), List(2, 2))
      val expected = Seq(List[Int]())

      val result = sc.parallelize(data).emptyF(_ <= 1)
      result should containInAnyOrder(expected)
    }
  }

  it should "support collectF" in {
    runWithContext { sc =>
      val data = Seq(List(1, 2, 3, 4))
      val expected = Seq(List.empty[String])

      val result = sc.parallelize(data).collectF {
        case 0 => "one"
      }

      result should containInAnyOrder(expected)
    }
  }

  it should "support productF" in {
    runWithContext { sc =>
      val data = Seq(Option(1))
      val expected = Seq(Option((1, 2)))

      val result = sc.parallelize(data).productF(_ + 1)

      result should containInAnyOrder(expected)
    }
  }

  it should "support mproductF" in {
    runWithContext { sc =>
      val data = Seq(List("12", "34", "56"))
      val expected =
        Seq(List(("12", '1'), ("12", '2'), ("34", '3'), ("34", '4'), ("56", '5'), ("56", '6')))

      val result = sc.parallelize(data).mproductF(_.toList)
      result should containInAnyOrder(expected)
    }
  }

  it should "support mapF nested" in {
    runWithContext { sc =>
      val data = Seq(List(Some(1), None, Some(2)))
      val expected = Seq(List(Some(2), None, Some(3)))
      val result = sc.parallelize(data).mapF(_ + 1)

      result should containInAnyOrder(expected)
    }
  }

  it should "support traverse" in {
    runWithContext { sc =>
      val data = Seq(List(Some(1), Some(2), None), List(Some(1), Some(2), Some(3)))
      val expected = Seq(None, Some(List(1, 2, 3)))

      val result = sc.parallelize(data).traverse(identity)

      result should containInAnyOrder(expected)
    }
  }

  it should "support flatTraverse" in {
    runWithContext { sc =>
      val data = Seq(Option(List("1", "2", "3", "four")))
      val expected = Seq(List(Some(1), Some(2), Some(3), None))
      val parseInt: String => Option[Int] = s => Try(s.toInt).toOption

      val result = sc.parallelize(data).flatTraverse(_.map(parseInt))

      result should containInAnyOrder(expected)
    }
  }

  it should "support sequence" in {
    runWithContext { sc =>
      val data = Seq(List(Option(1), Option(2)))
      val expected: Seq[Option[List[Int]]] = Seq(Some(List(1, 2)))
      val result = sc.parallelize(data).sequence

      result should containInAnyOrder(expected)
    }
  }

  it should "do wordcount" in {
    runWithContext { sc =>
      val data = Seq(List("1", "foo", "2"))
      // val data: Seq[Either[String, String]] =
      //   Seq(Right("1"), Right("foo"), Right("2"), Left("ERROR"))
      val expected = Seq((3, 5, Map("1" -> 1, "foo" -> 1, "2" -> 1)))

      val result = sc
        .parallelize(data)
        .mapF(s => (1, s.length, Map(s -> 1)))
        .flatten_
        .fold_

      result should containInAnyOrder(expected)
    }
  }

}
