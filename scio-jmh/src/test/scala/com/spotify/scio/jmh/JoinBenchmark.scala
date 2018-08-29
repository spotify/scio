/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.jmh

import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import com.google.common.collect.AbstractIterator
import com.google.common.collect.Lists
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._

// scalastyle:off number.of.methods
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class JoinBenchmark {

  private def genIterable(n: Int): JIterable[Int] = {
    val l = Lists.newArrayList[Int]()
    (1 to n).foreach(l.add)
    l
  }

  private val i1 = genIterable(1)
  private val i10 = genIterable(10)
  private val i100 = genIterable(100)

  @Benchmark def forYieldA(bh: Blackhole): Unit = forYield(i1, i10, bh)
  @Benchmark def forYieldB(bh: Blackhole): Unit = forYield(i10, i1, bh)
  @Benchmark def forYieldC(bh: Blackhole): Unit = forYield(i10, i100, bh)
  @Benchmark def forYieldD(bh: Blackhole): Unit = forYield(i100, i10, bh)

  @Benchmark def artisanA(bh: Blackhole): Unit = artisan(i1, i10, bh)
  @Benchmark def artisanB(bh: Blackhole): Unit = artisan(i10, i1, bh)
  @Benchmark def artisanC(bh: Blackhole): Unit = artisan(i10, i100, bh)
  @Benchmark def artisanD(bh: Blackhole): Unit = artisan(i100, i10, bh)

  @Benchmark def muchArtisanA(bh: Blackhole): Unit = muchArtisan(i1, i10, bh)
  @Benchmark def muchArtisanB(bh: Blackhole): Unit = muchArtisan(i10, i1, bh)
  @Benchmark def muchArtisanC(bh: Blackhole): Unit = muchArtisan(i10, i100, bh)
  @Benchmark def muchArtisanD(bh: Blackhole): Unit = muchArtisan(i100, i10, bh)

  @Benchmark def suchArtisanA(bh: Blackhole): Unit = suchArtisan(i1, i10, bh)
  @Benchmark def suchArtisanB(bh: Blackhole): Unit = suchArtisan(i10, i1, bh)
  @Benchmark def suchArtisanC(bh: Blackhole): Unit = suchArtisan(i10, i100, bh)
  @Benchmark def suchArtisanD(bh: Blackhole): Unit = suchArtisan(i100, i10, bh)

  @Benchmark def iteratorA(bh: Blackhole): Unit = iterator(i1, i10, bh)
  @Benchmark def iteratorB(bh: Blackhole): Unit = iterator(i10, i1, bh)
  @Benchmark def iteratorC(bh: Blackhole): Unit = iterator(i10, i100, bh)
  @Benchmark def iteratorD(bh: Blackhole): Unit = iterator(i100, i10, bh)

  def forYield(as: JIterable[Int], bs: JIterable[Int], bh: Blackhole): Unit =
    forYield(as, bs, new BlackholeContext[(Int, Int)](bh))

  def forYield(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    val xs: TraversableOnce[(Int, Int)] =
      for (a <- as.asScala.iterator; b <- bs.asScala.iterator) yield (a, b)
    val i = xs.toIterator
    while (i.hasNext) c.output(i.next())
  }

  def artisan(as: JIterable[Int], bs: JIterable[Int], bh: Blackhole): Unit =
    artisan(as, bs, new BlackholeContext[(Int, Int)](bh))

  def artisan(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    (peak(as), peak(bs)) match {
      case ((1, a), (1, b)) => c.output((a, b))
      case ((1, a), (2, _)) =>
        val i = bs.iterator()
        while (i.hasNext) c.output((a, i.next()))
      case ((2, _), (1, b)) =>
        val i = as.iterator()
        while (i.hasNext) c.output((i.next(), b))
      case ((2, _), (2, _)) =>
        val i = as.iterator()
        while (i.hasNext) {
          val a = i.next()
          val j = bs.iterator()
          while (j.hasNext) {
            c.output((a, j.next()))
          }
        }
      case _ => ()
    }
  }

  def muchArtisan(as: JIterable[Int], bs: JIterable[Int], bh: Blackhole): Unit =
    muchArtisan(as, bs, new BlackholeContext[(Int, Int)](bh))

  // scalastyle:off cyclomatic.complexity
  def muchArtisan(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    var ai = as.iterator()
    var bi = bs.iterator()
    if (ai.hasNext && bi.hasNext) {
      var a = ai.next()
      val b = bi.next()
      val a1 = !ai.hasNext
      val b1 = !bi.hasNext
      if (a1 && b1) {
        c.output((a, b))
      } else if (a1 && !b1) {
        c.output((a, b))
        c.output((a, bi.next()))
        while (bi.hasNext) {
          c.output((a, bi.next()))
        }
      } else if (!a1 && b1) {
        c.output((a, b))
        c.output((ai.next(), b))
        while (ai.hasNext) {
          c.output((ai.next(), b))
        }
      } else {
        ai = as.iterator()
        while (ai.hasNext) {
          a = ai.next()
          bi = bs.iterator()
          while (bi.hasNext) {
            c.output((a, bi.next()))
          }
        }
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  def suchArtisan(as: JIterable[Int], bs: JIterable[Int], bh: Blackhole): Unit =
    suchArtisan(as, bs, new BlackholeContext[(Int, Int)](bh))

  def suchArtisan(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    val ai = as.iterator()
    while (ai.hasNext) {
      val a = ai.next()
      val bi = bs.iterator()
      while (bi.hasNext) {
        c.output((a, bi.next()))
      }
    }
  }

  def iterator(as: JIterable[Int], bs: JIterable[Int], bh: Blackhole): Unit =
    iterator(as, bs, new BlackholeContext[(Int, Int)](bh))

  def iterator(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    val i = new CartesianIterator(as, bs)
    while (i.hasNext) {
      c.output(i.next())
    }
  }

  @inline private def peak[A](xs: java.lang.Iterable[A]): (Int, A) = {
    val i = xs.iterator()
    if (i.hasNext) {
      val a = i.next()
      if (i.hasNext) (2, null.asInstanceOf[A]) else (1, a)
    } else {
      (0, null.asInstanceOf[A])
    }
  }

  test(i1, i1)
  test(i1, i10)
  test(i1, i100)
  test(i10, i1)
  test(i10, i10)
  test(i10, i100)
  test(i100, i1)
  test(i100, i10)
  test(i100, i100)

  private def test(as: JIterable[Int], bs: JIterable[Int]): Unit = {
    val c1 = new SeqContext[(Int, Int)]
    forYield(as, bs, c1)
    val c2 = new SeqContext[(Int, Int)]
    artisan(as, bs, c2)
    val c3 = new SeqContext[(Int, Int)]
    iterator(as, bs, c3)
    val c4 = new SeqContext[(Int, Int)]
    muchArtisan(as, bs, c4)
    val c5 = new SeqContext[(Int, Int)]
    suchArtisan(as, bs, c5)
    require(c1.result == c2.result)
    require(c1.result == c3.result)
    require(c1.result == c4.result)
    require(c1.result == c5.result)
  }

  trait Context[T] {
    def output(x: T): Unit
    def result: Seq[T]
  }

  class BlackholeContext[T](val blackhole: Blackhole) extends Context[T] {
    override def output(x: T): Unit = blackhole.consume(x)
    override def result: Seq[T] = Nil
  }

  class SeqContext[T] extends Context[T] {
    private val b = Seq.newBuilder[T]
    override def output(x: T): Unit = b += x
    override def result: Seq[T] = b.result()
  }

}

private class CartesianIterator[A, B](as: JIterable[A], bs: JIterable[B])
  extends AbstractIterator[(A, B)] {
  private val asi = as.iterator()
  private var bsi = bs.iterator()
  private var a: A = _

  if (asi.hasNext) {
    a = asi.next()
  } else {
    endOfData()
  }

  override def computeNext(): (A, B) = {
    if (!bsi.hasNext) {
      if (!asi.hasNext)  {
        endOfData()
      } else {
        a = asi.next()
        bsi = bs.iterator

        if (!bsi.hasNext) {
          endOfData()
        } else {
          (a, bsi.next())
        }
      }
    } else {
      (a, bsi.next())
    }
  }
}
// scalastyle:on number.of.methods
