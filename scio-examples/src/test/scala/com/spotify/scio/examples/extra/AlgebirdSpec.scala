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

package com.spotify.scio.examples.extra

import com.twitter.algebird._
import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck._

object AlgebirdSpec extends Properties("Algebird")  {

  /** Add Algebird support for Iterable[T]. */
  implicit class AlgebirdIterable[T](self: Iterable[T]) {

    /** Sum with an implicit Semigroup. */
    def algebirdSum(implicit sg: Semigroup[T]): T = self.reduce(sg.plus)

    /**
     * Aggregate with an implicit Aggregator.
     * @tparam A intermediate type that can be summed with a Semigroup
     * @tparam U result type
     */
    def algebirdAggregate[A, U](aggregator: Aggregator[T, A, U]): U = {
      val a = self
        .map(aggregator.prepare)
        .algebirdSum(aggregator.semigroup)
      aggregator.present(a)
    }

  }

  // Properties using arbitrary generators and conditionals

  property("sum of Int") = forAll { xs: List[Int] =>
    xs.nonEmpty ==> (xs.algebirdSum == xs.sum)
  }

  property("sum of Long") = forAll { xs: List[Long] =>
    xs.nonEmpty ==> (xs.algebirdSum == xs.sum)
  }

  property("sum of Float") = forAll { xs: List[Float] =>
    xs.nonEmpty ==> (xs.algebirdSum == xs.sum)
  }

  property("sum of Double") = forAll { xs: List[Double] =>
    xs.nonEmpty ==> (xs.algebirdSum == xs.sum)
  }

  property("sum of Set") = forAll { xs: List[Set[String]] =>
    xs.nonEmpty ==> (xs.algebirdSum == xs.reduce(_ ++ _))
  }

  // Compose a tuple generator from existing arbitrary generators
  val tupleGen = for {
    i <- Arbitrary.arbitrary[Int]
    d <- Arbitrary.arbitrary[Double]
    s <- Arbitrary.arbitrary[Set[String]]
  } yield (i, d, s)

  property("sum of tuples") = forAll(Gen.nonEmptyListOf(tupleGen)) { xs =>
    xs.algebirdSum == (xs.map(_._1).sum, xs.map(_._2).sum, xs.map(_._3).reduce(_ ++ _))
  }

  property("sum of tuples with custom Semigroup") = forAll { xs: List[(Double, Double, Double)] =>
    xs.nonEmpty ==> {
      // Apply sum, max, and min operation on the 3 columns
      val sumOp = Semigroup.doubleSemigroup
      val maxOp = MaxAggregator[Double].semigroup
      val minOp = MinAggregator[Double].semigroup
      // Combine 3 Semigroup[Double] into 1 Semigroup[(Double, Double, Double)]
      val colSg = Semigroup.semigroup3(sumOp, maxOp, minOp)
      xs.algebirdSum(colSg) == (xs.map(_._1).sum, xs.map(_._2).max, xs.map(_._3).min)
    }
  }

  property("aggregate of tuples with custom Aggregator") =
    forAll { xs: List[(Double, Double, Double, Double)] =>
      xs.nonEmpty ==> {
        type C = (Double, Double, Double, Double)
        // Apply sum, max, min, and average operation on the 4 columns
        val sumOp = Aggregator.prepareMonoid[C, Double](_._1)
        val maxOp = Aggregator.maxBy[C, Double](_._2)
        val minOp = Aggregator.minBy[C, Double](_._3)
        val avgOp = AveragedValue.aggregator.composePrepare[C](_._4)
        // Combine 4 Aggregator[C, Double, Double] into 1 Aggregator[C, C, C]
        val colAgg = MultiAggregator(sumOp, maxOp, minOp, avgOp)
        xs.algebirdAggregate(colAgg) ==
          (xs.map(_._1).sum, xs.map(_._2).max, xs.map(_._3).min, xs.map(_._4).sum / xs.size)
      }
    }

  case class Record(i: Int, d: Double, s: Set[String])

  // Map a tuple generator to a case class generator
  val recordGen = tupleGen.map(Record.tupled)

  // Semigroup for a case class
  val recordSg = Semigroup(Record.apply _, Record.unapply _)

  property("sum of case classes") = forAll(Gen.nonEmptyListOf(recordGen)) { xs =>
    xs.algebirdSum(recordSg) == xs.reduce((a, b) => Record(a.i + b.i, a.d + b.d, a.s ++ b.s))
  }

  // HyperLogLog for approximate distinct count

  property("sum with HyperLogLog") = forAll(Gen.nonEmptyListOf(Gen.alphaStr)) { xs =>
    val m = new HyperLogLogMonoid(10)
    xs.map(i => m.create(i.getBytes))
      .algebirdSum(m)
      .approximateSize
      .boundsContain(xs.toSet.size)
  }

  property("aggregate with HyperLogLog") = forAll(Gen.nonEmptyListOf(Gen.alphaStr)) { xs =>
    xs.algebirdAggregate(HyperLogLogAggregator(10).composePrepare(_.getBytes))
      .approximateSize.boundsContain(xs.toSet.size)
  }

  // BloomFilter for approximate set membership

  property("sum with BloomFilter") = forAll { xs: List[String] =>
    xs.nonEmpty ==> {
      val m = BloomFilter(1000, 0.01)
      val bf = xs
        .map(m.create)
        .algebirdSum(m)
      xs.forall(bf.contains(_).isTrue)
    }
  }

  property("aggregator with BloomFilter") = forAll { xs: List[String] =>
    xs.nonEmpty ==> {
      val width = BloomFilter.optimalWidth(1000, 0.01)
      val numHashes = BloomFilter.optimalNumHashes(1000, width)
      val m = BloomFilter(1000, 0.01)
      val bf = xs.algebirdAggregate(BloomFilterAggregator(numHashes, width))
      xs.forall(bf.contains(_).isTrue)
    }
  }

  // QTree for approximate quantiles

  property("sum with QTree") = forAll(Gen.listOfN(1000, Gen.posNum[Int])) { xs =>
    val qt = xs
      .map(QTree(_))
      .algebirdSum(new QTreeSemigroup[Long](10))
    val l = xs.length
    val bounds = Seq(0.25, 0.50, 0.75).map(qt.quantileBounds)
    val expected = Seq(l / 4, l / 2, l / 4 * 3).map(xs.sorted)
    bounds.zip(expected).forall { case ((lower, upper), x) =>
      lower <= x && x <= upper
    }
  }

  property("aggregate with QTree") = forAll(Gen.listOfN(1000, Gen.posNum[Int])) { xs =>
    val l = xs.length
    val s = xs.sorted
    val bounds = Seq(0.25, 0.50, 0.75).map(p => xs.algebirdAggregate(QTreeAggregator(p, 10)))
    val expected = Seq(l / 4, l / 2, l / 4 * 3).map(xs.sorted)
    bounds.zip(expected).forall { case (b, x) => b.contains(x) }
  }

  // CountMinSketch for approximate frequency

  property("sum with CountMinSketch") = forAll(Gen.listOfN(1000, Gen.alphaStr)) { xs =>
    import CMSHasherImplicits._
    val m = CMS.monoid[String](0.001, 1e-10, 1)
    val cms = xs.map(m.create).algebirdSum(m)
    val expected = xs.groupBy(identity).mapValues(_.size)
    expected.forall { case (item, freq) =>
      cms.frequency(item).contains(freq).isTrue
    }
  }

  property("aggregate with CountMinSketch") = forAll(Gen.listOfN(1000, Gen.alphaStr)) { xs =>
    import CMSHasherImplicits._
    val cms = xs.algebirdAggregate(CMS.aggregator(0.01, 1e-10, 1))
    val expected = xs.groupBy(identity).mapValues(_.size)
    expected.forall { case (item, freq) =>
      cms.frequency(item).contains(freq).isTrue
    }
  }

  // DecayedValue for moving average

  val timeSeries = Gen.listOfN(1000, Gen.posNum[Double]).map(_.zipWithIndex)

  property("sum with DecayedValue") = forAll(timeSeries) { xs =>
    val halfLife = 10.0
    val decayFactor = math.exp(math.log(0.5) / halfLife)
    val normalization = halfLife / math.log(2)
    val expected = xs.map(_._1).reduce(_ * decayFactor + _) / normalization
    val actual = xs.map { case (v, t) => DecayedValue.build(v, t, halfLife) }
      .algebirdSum(DecayedValue.monoidWithEpsilon(1e-3))
      .average(halfLife)
    math.abs(actual - expected) <= 1e-3
  }

  property("aggregate with DecayedValue") = forAll(timeSeries) { xs =>
    val halfLife = 10.0
    val decayFactor = math.exp(math.log(0.5) / halfLife)
    val normalization = halfLife / math.log(2)
    val expected = xs.map(_._1).reduce(_ * decayFactor + _) / normalization
    val actual = xs.algebirdAggregate(
      Aggregator
        .fromMonoid(DecayedValue.monoidWithEpsilon(1e-3))
        .composePrepare { case (v, t) => DecayedValue.build(v, t, halfLife) })
      .average(halfLife)
    math.abs(actual - expected) <= 1e-3
  }

}
