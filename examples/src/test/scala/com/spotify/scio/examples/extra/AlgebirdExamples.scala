package com.spotify.scio.examples.extra

import com.spotify.scio.testing.PipelineSpec
import com.twitter.algebird._

import scala.util.Random

class AlgebirdExamples extends PipelineSpec {

  "SCollection" should "support sum with primitive types" in {
    val s = 1 to 10
    runWithData(s)(_.sum).head shouldBe 55
    runWithData(s.map(_.toLong))(_.sum).head shouldBe 55L
    runWithData(s.map(_.toFloat))(_.sum).head shouldBe 55F
    runWithData(s.map(_.toDouble))(_.sum).head shouldBe 55.0
  }

  it should "support sum with tuples" in {
    val s = Seq.fill(1000)(Random.nextInt(100)) zip Seq.fill(1000)(Random.nextString(10))

    val output = runWithData(s) { p =>
      p.map(t => (t._1, Set(t._2))).sum
    }.head
    output._1 should equal (s.map(_._1).sum)
    output._2 should equal (s.map(_._2).toSet)
  }

  it should "support sum and aggregate with HyperLogLog" in {
    val s = Seq.fill(1000)(Random.nextInt(100))
    val expected = s.groupBy(identity).size

    runWithData(s) { p =>
      val m = new HyperLogLogMonoid(10)
      // HyperLogLog requires Array[Byte]
      p.map(i => m.create(i.toString.getBytes)).sum(m)
    }.head.approximateSize.boundsContain(expected) shouldBe true

    runWithData(s) {
      // HyperLogLog requires Array[Byte]
      _.aggregate(HyperLogLogAggregator(10).composePrepare(_.toString.getBytes))
    }.head.approximateSize.boundsContain(expected) shouldBe true
  }

  it should "support sum and aggregate with BloomFilter" in {
    val s = Seq.fill(1000)(Random.nextString(10))

    val bf1 = runWithData(s) { p =>
      val m = BloomFilter(1000, 0.01)
      p.map(m.create).sum(m)
    }.head
    all (s.map(bf1.contains(_).isTrue)) shouldBe true

    val bf2 = runWithData(s) { p =>
      val width = BloomFilter.optimalWidth(1000, 0.01)
      val numHashes = BloomFilter.optimalNumHashes(1000, width)
      p.aggregate(BloomFilterAggregator(numHashes, width))
    }.head
    all (s.map(bf2.contains(_).isTrue)) shouldBe true
  }

  it should "support sum and aggregate with QTree" in {
    def contains(qt: QTree[Long], p: Double)(v: Long): Boolean = {
      val (l, u) = qt.quantileBounds(p)
      l <= v && v <= u
    }
    val numbers = Seq(250, 500, 750)

    val q = runWithData(1 to 1000) {
      _.map(QTree(_)).sum(new QTreeSemigroup[Long](10))
    }.head
    // each quantile bound should fit only one of the numbers
    numbers.map(contains(q, 0.25)(_)) should equal (Seq(true, false, false))
    numbers.map(contains(q, 0.50)(_)) should equal (Seq(false, true, false))
    numbers.map(contains(q, 0.75)(_)) should equal (Seq(false, false, true))

    val i1 = runWithData(1 to 1000) {
      _.aggregate(QTreeAggregator(0.25, 10))
    }.head
    numbers.map(i1.contains(_)) should equal (Seq(true, false, false))

    val i2 = runWithData(1 to 1000) {
      _.aggregate(QTreeAggregator(0.50, 10))
    }.head
    numbers.map(i2.contains(_)) should equal (Seq(false, true, false))

    val i3 = runWithData(1 to 1000) {
      _.aggregate(QTreeAggregator(0.75, 10))
    }.head
    numbers.map(i3.contains(_)) should equal (Seq(false, false, true))
  }

  it should "support sum and aggregate with CountMinSketch" in {
    import CMSHasherImplicits._
    val s = Seq.fill(1000)(Random.nextInt(100))
    val expected = s.groupBy(identity).mapValues(_.size)

    val cms1 = runWithData(s) { p =>
      val m = CMS.monoid[Int](0.001, 1e-10, 1)
      p.map(m.create).sum(m)
    }.head
    all (expected.map(kv => cms1.frequency(kv._1).boundsContain(kv._2))) shouldBe true

    val cms2 = runWithData(s) {
      _.aggregate(CMS.aggregator[Int](0.001, 1e-10, 1))
    }.head
    all (expected.map(kv => cms2.frequency(kv._1).boundsContain(kv._2))) shouldBe true
  }

  it should "support sum and aggregate with DecayedValue" in {
    val s = (5001 to 6000).map(_.toDouble).zipWithIndex

    val halfLife = 10.0
    val decayFactor = math.exp(math.log(0.5) / halfLife)
    val normalization = halfLife / math.log(2)
    val expected = s.map(_._1).reduce(_ * decayFactor + _) / normalization

    runWithData(s) {
      _
        .map { case (v, t) => DecayedValue.build(v, t, 10.0) }
        .sum(DecayedValue.monoidWithEpsilon(1e-3))
    }.head.average(10.0) shouldBe expected +- 1e-3

    runWithData(s) {
      _.aggregate(
        Aggregator
          .fromMonoid(DecayedValue.monoidWithEpsilon(1e-3))
          .composePrepare { case (v, t) => DecayedValue.build(v, t, 10.0) })
    }.head.average(10.0) shouldBe expected +- 1e-3
  }

}
