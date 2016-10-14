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

object ForeachBenchmark extends Bench.LocalTime {

  val sizes = Gen.enumeration("size")(100000, 500000, 1000000)

  def jIterable(i: Int): JIterable[String] =
    Lists.newArrayList((0 until i).map("v%05d".format(_)): _*).asInstanceOf[JIterable[String]]

  val inputs = for (s <- sizes) yield jIterable(s)

  performance of "Foreach" in {
    measure method "foreach" in {
      using(inputs) in { case (in) =>
        in.asScala.foreach(_ => Unit)
      }
    }

    measure method "forLoop" in {
      using(inputs) in { case (in) =>
        for (i <- in.asScala) Unit
      }
    }

    measure method "whileLoop" in {
      using(inputs) in { case (in) =>
        val i = in.iterator()
        while (i.hasNext) i.next()
      }
    }
  }

}
