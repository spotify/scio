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

package com.spotify.scio.examples.extra

import com.twitter.algebird._
import org.scalacheck._
import org.scalatest.Ignore
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

@Ignore
class AlgebirdSpec extends AnyPropSpec with ScalaCheckDrivenPropertyChecks with Matchers {
  // Default minSuccessful is 10 instead of 100 in ScalaCheck but that should be enough
  // https://github.com/scalatest/scalatest/issues/1090 is addressed

  // =======================================================================
  // Utilities
  // =======================================================================

  /**
   * Mimic an SCollection using standard List.
   *
   * This is faster and more readable than creating ScioContext and SCollection repeatedly in
   * ScalaCheck properties.
   */
  class SColl[T](val internal: List[T]) {

    /** Sum with an implicit Semigroup. */
    def sum(implicit sg: Semigroup[T]): T = internal.reduce(sg.plus)

    /**
     * Aggregate with an implicit Aggregator.
     *
     * @tparam A
     *   intermediate type that can be summed with a Semigroup
     * @tparam U
     *   result type
     */
    def aggregate[A, U](aggregator: Aggregator[T, A, U]): U = {
      val a = internal
        .map(aggregator.prepare)
        .reduce(aggregator.semigroup.plus)
      aggregator.present(a)
    }

    def map[U](f: T => U): SColl[U] = new SColl(internal.map(f))

    override def toString: String = internal.mkString("[", ", ", "]")
  }

  // Generator for non-empty SColl[T]
  def sCollOf[T](g: => Gen[T]): Gen[SColl[T]] =
    Gen.nonEmptyListOf(g).map(new SColl(_))

  // Generator for SColl[T] of given length
  def sCollOfN[T](n: Int, g: => Gen[T]): Gen[SColl[T]] =
    Gen.listOfN(n, g).map(new SColl(_))

  // Arbitrary for non-empty SColl[T]
  implicit def arbSColl[T](implicit a: Arbitrary[T]): Arbitrary[SColl[T]] =
    Arbitrary(sCollOf(a.arbitrary))

  // =======================================================================
  // Basic examples
  // =======================================================================

  property("sum of Int") {
    forAll { xs: SColl[Int] => xs.sum shouldBe xs.internal.sum }
  }

  property("sum of Long") {
    forAll { xs: SColl[Long] => xs.sum shouldBe xs.internal.sum }
  }

  property("sum of Float") {
    forAll { xs: SColl[Float] => xs.sum shouldBe xs.internal.sum }
  }

  property("sum of Double") {
    forAll { xs: SColl[Double] => xs.sum shouldBe xs.internal.sum }
  }

  property("sum of Set") {
    forAll { xs: SColl[Set[String]] => xs.sum shouldBe xs.internal.reduce(_ ++ _) }
  }

  // Sum fields of tuples individually
  property("sum of tuples") {
    forAll { xs: SColl[(Int, Double, Set[String])] =>
      val expected =
        (xs.internal.map(_._1).sum, xs.internal.map(_._2).sum, xs.internal.map(_._3).reduce(_ ++ _))
      xs.sum shouldBe expected
    }
  }

  property("sum of tuples with custom Semigroup") {
    forAll { xs: SColl[(Double, Double, Double)] =>
      // Apply sum, max, and min operation on the 3 columns
      val sumOp = Semigroup.doubleSemigroup
      val maxOp = MaxAggregator[Double]().semigroup
      val minOp = MinAggregator[Double]().semigroup
      // Combine 3 Semigroup[Double] into 1 Semigroup[(Double, Double, Double)]
      val colSg = Semigroup.semigroup3(sumOp, maxOp, minOp)

      val expected =
        (xs.internal.map(_._1).sum, xs.internal.map(_._2).max, xs.internal.map(_._3).min)
      xs.sum(colSg) shouldBe expected
    }
  }

  def mean(xs: List[Double]): Double = xs.sum / xs.size

  def stddev(xs: List[Double]): Double =
    if (xs.size > 1) {
      val mean = xs.sum / xs.size
      math.sqrt(xs.map(x => math.pow(x - mean, 2)).sum / xs.size)
    } else {
      Double.NaN
    }

  // x or y could be NaN, Infinity or NegativeInfinity
  def error(x: Double, y: Double): Double = {
    val e = (x - y) / math.max(x, y)
    if (e.isWhole) e else 0.0
  }

  property("aggregate of tuples with custom Aggregator") {
    forAll { xs: SColl[(Double, Double, Double, Double)] =>
      type C = (Double, Double, Double, Double)
      // Apply sum, max, min, and average operation on the 4 columns
      // Average cannot be performed as a Semigroup
      val sumOp = Aggregator.prepareMonoid[C, Double](_._1)
      val maxOp = Aggregator.max[Double].composePrepare[C](_._2)
      val minOp = Aggregator.min[Double].composePrepare[C](_._3)
      val avgOp = AveragedValue.aggregator.composePrepare[C](_._4)
      // Combine 4 Aggregator[C, Double, Double] into 1 Aggregator[C, C, C]
      val colAgg = MultiAggregator((sumOp, maxOp, minOp, avgOp))

      val expected = (
        xs.internal.map(_._1).sum,
        xs.internal.map(_._2).max,
        xs.internal.map(_._3).min,
        mean(xs.internal.map(_._4))
      )
      val actual = xs.aggregate(colAgg)
      actual._1 shouldBe expected._1
      actual._2 shouldBe expected._2
      actual._3 shouldBe expected._3
      error(actual._4, expected._4) shouldBe 0.0 +- 1e-10 // double arithmetic error
    }
  }

  property("aggregate of Double with multiple aggregators") {
    forAll { xs: SColl[Double] =>
      // Apply max, min, and moments aggregator on the same Double
      val maxOp = Aggregator.max[Double]
      val minOp = Aggregator.min[Double]
      val momentsOp = Moments.aggregator
      val colAgg = MultiAggregator((maxOp, minOp, momentsOp))
        .andThenPresent { case (max, min, moments) =>
          // Present mean and stddev in Moments
          (max, min, moments.mean, moments.stddev)
        }

      val expected = (xs.internal.max, xs.internal.min, mean(xs.internal), stddev(xs.internal))
      val actual = xs.aggregate(colAgg)
      actual._1 shouldBe expected._1
      actual._2 shouldBe expected._2
      // double arithmetic error
      error(actual._3, expected._3) shouldBe 0.0 +- 1e-10
      error(actual._4, expected._4) shouldBe 0.0 +- 1e-10
    }
  }

  case class Record(i: Int, d: Double, s: Set[String])

  // Compose a Record generator from existing arbitrary generators
  val recordGen: Gen[Record] = for {
    i <- Arbitrary.arbitrary[Int]
    d <- Arbitrary.arbitrary[Double]
    s <- Arbitrary.arbitrary[Set[String]]
  } yield Record(i, d, s)

  property("sum of case classes") {
    // Macros that automatically derive Algebird type for case classes
    import com.twitter.algebird.macros._

    implicit val recordSemigroup: Semigroup[Record] = caseclass.semigroup

    forAll(sCollOf(recordGen)) { xs =>
      val expected = Record(
        xs.internal.map(_.i).sum,
        xs.internal.map(_.d).sum,
        xs.internal.map(_.s).reduce(_ ++ _)
      )
      xs.sum shouldBe expected
    }
  }

  // =======================================================================
  // HyperLogLog for approximate distinct count
  // =======================================================================

  val hllBits = 8
  val hllError: Double = 1.04 / math.sqrt(math.pow(2, hllBits.toDouble)) // 0.065
  val hllInput: Gen[List[SColl[String]]] = Gen.listOfN(100, sCollOf(Gen.alphaStr))

  property("sum with HyperLogLog") {
    forAll(hllInput) { xss =>
      val m = new HyperLogLogMonoid(hllBits)
      val e = xss.count { xs =>
        val pass = xs
          .map(i => m.create(i.getBytes))
          .sum(m)
          .approximateSize
          // approximate bounds should contain exact distinct count
          .boundsContain(xs.internal.toSet.size.toLong)
        !pass
      }.toDouble / xss.size
      e should be < hllError
    }
  }

  property("aggregate with HyperLogLog") {
    forAll(hllInput) { xss =>
      val e = xss.count { xs =>
        val pass = xs
          .aggregate(HyperLogLogAggregator(hllBits).composePrepare(_.getBytes))
          .approximateSize
          // approximate bounds should contain exact distinct count
          .boundsContain(xs.internal.toSet.size.toLong)
        !pass
      }.toDouble / xss.size
      e should be < hllError
    }
  }

  // =======================================================================
  // BloomFilter for approximate set membership
  // =======================================================================

  property("sum with BloomFilter") {
    forAll { xs: SColl[String] =>
      val m = BloomFilter[String](1000, 0.01)
      val bf = xs
        .map(m.create)
        .sum(m)
      // BF should test positive for all members
      xs.internal.forall(bf.contains(_).isTrue) shouldBe true
    }
  }

  property("aggregator with BloomFilter") {
    forAll { xs: SColl[String] =>
      val width = BloomFilter.optimalWidth(1000, 0.01).get
      val numHashes = BloomFilter.optimalNumHashes(1000, width)
      val bf = xs.aggregate(BloomFilterAggregator(numHashes, width))
      // BF should test positive for all members
      xs.internal.forall(bf.contains(_).isTrue)
    }
  }

  // =======================================================================
  // QTree for approximate quantiles
  // =======================================================================

  // Generator for SColl[Int]
  val posInts: Gen[SColl[Int]] = Gen.listOfN(1000, Gen.posNum[Int]).map(new SColl(_))

  property("sum with QTree") {
    forAll(posInts) { xs =>
      val qt = xs
        .map(x => QTree(x.toLong))
        .sum(new QTreeSemigroup[Long](10))
      val l = xs.internal.length
      val bounds = Seq(0.25, 0.50, 0.75).map(qt.quantileBounds)
      val expected = Seq(l / 4, l / 2, l / 4 * 3).map(xs.internal.sorted)
      // approximate bounds should contain exact 25, 50 and 75 percentile
      bounds.zip(expected).forall { case ((lower, upper), x) =>
        lower <= x && x <= upper
      } shouldBe true
    }
  }

  property("aggregate with QTree") {
    forAll(posInts) { xs =>
      val l = xs.internal.length
      val s = xs.internal.sorted
      val bounds =
        Seq(0.25, 0.50, 0.75).map(p => xs.aggregate(QTreeAggregator(p, 10)))
      val expected = Seq(l / 4, l / 2, l / 4 * 3).map(s)
      // approximate bounds should contain exact 25, 50 and 75 percentile
      bounds.zip(expected).forall { case (b, x) => b.contains(x.toDouble) } shouldBe true
    }
  }

  // =======================================================================
  // CountMinSketch for approximate frequency
  // =======================================================================

  property("sum with CountMinSketch") {
    forAll(sCollOfN(1000, Gen.alphaStr)) { xs =>
      import CMSHasherImplicits._
      val m = CMS.monoid[String](0.001, 1e-10, 1)
      val cms = xs.map(m.create).sum(m)
      val expected = xs.internal.groupBy(identity).mapValues(_.size)
      expected.forall { case (item, freq) =>
        // approximate bounds of each item should contain exact frequency
        cms.frequency(item).boundsContain(freq.toLong)
      } shouldBe true
    }
  }

  property("aggregate with CountMinSketch") {
    forAll(sCollOfN(1000, Gen.alphaStr)) { xs =>
      import CMSHasherImplicits._
      val cms = xs.aggregate(CMS.aggregator(0.01, 1e-10, 1))
      val expected = xs.internal.groupBy(identity).mapValues(_.size)
      expected.forall { case (item, freq) =>
        // approximate bounds of each item should contain exact frequency
        cms.frequency(item).boundsContain(freq.toLong)
      } shouldBe true
    }
  }

  // =======================================================================
  // DecayedValue for moving average
  // =======================================================================

  // Generator for SColl[(Double, Int)]
  val timeSeries: Gen[SColl[(Double, Int)]] =
    Gen.listOfN(1000, Gen.posNum[Double]).map(_.zipWithIndex).map(new SColl(_))

  property("sum with DecayedValue") {
    forAll(timeSeries) { xs =>
      val halfLife = 10.0
      val decayFactor = math.exp(math.log(0.5) / halfLife)
      val normalization = halfLife / math.log(2)
      val expected = xs.internal
        .map(_._1)
        .reduce(_ * decayFactor + _) / normalization
      val actual = xs
        .map { case (v, t) => DecayedValue.build(v, t.toDouble, halfLife) }
        .sum(DecayedValue.monoidWithEpsilon(1e-3))
        .average(halfLife)
      // approximate decayed value should be close to exact value
      actual shouldBe expected +- 1e-3
    }
  }

  property("aggregate with DecayedValue") {
    forAll(timeSeries) { xs =>
      val halfLife = 10.0
      val decayFactor = math.exp(math.log(0.5) / halfLife)
      val normalization = halfLife / math.log(2)
      val expected = xs.internal
        .map(_._1)
        .reduce(_ * decayFactor + _) / normalization
      val actual = xs
        .aggregate(
          Aggregator
            .fromMonoid(DecayedValue.monoidWithEpsilon(1e-3))
            .composePrepare { case (v, t) => DecayedValue.build(v, t.toDouble, halfLife) }
        )
        .average(halfLife)
      // approximate decayed value should be close to exact value
      actual shouldBe expected +- 1e-3
    }
  }
}
