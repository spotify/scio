/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bench

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.collection.JavaConverters._

/** Micro-benchmark for for/yield patterns used in MultiJoin.scala. */
object JoinBenchmark extends Bench.LocalTime {

  val sizes = Gen.enumeration("size")((1, 1), (1, 1000000), (1000000, 1), (1000, 1000))

  def jIterable(i: Int): JIterable[String] =
    Lists.newArrayList((0 until i).map("v%05d".format(_)): _*).asInstanceOf[JIterable[String]]

  val inputs = for (p <- sizes) yield (jIterable(p._1), jIterable(p._2))

  performance of "Join" in {
    measure method "forIterable" in {
      using(inputs) in { case (l, r) =>
        for {
          a <- l.asScala
          b <- r.asScala
        } yield ("key", (a, b))
      }
    }

    measure method "forIterator" in {
      using(inputs) in { case (l, r) =>
        val i = for {
          a <- l.asScala.iterator
          b <- r.asScala.iterator
        } yield ("key", (a, b))
        while (i.hasNext) i.next()  // eager iterator evaluation
      }
    }

    measure method "forLoop" in {
      using(inputs) in { case (l, r) =>
        val result = Lists.newArrayList[(String, (String, String))]()
        for {
          a <- l.asScala.iterator
          b <- r.asScala.iterator
        } result.add(("key", (a, b)))
      }
    }

    measure method "whileLoop" in {
      using(inputs) in { case (l, r) =>
        val result = Lists.newArrayList[(String, (String, String))]()
        val a = l.asScala.iterator
        while (a.hasNext) {
          val va = a.next()
          val b = r.asScala.iterator
          while (b.hasNext) {
            val vb = b.next()
            result.add(("key", (va, vb)))
          }
        }
      }
    }
  }

}
