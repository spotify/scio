package com.spotify.scio.examples.extra

import com.spotify.scio.testing.PipelineTest
import com.twitter.algebird._

import scala.util.Random

class AlgebirdExamples extends PipelineTest {

  def boundsContain(b: (Double, Double), actual: Double) = actual >= b._1 && actual <= b._2

  "SCollection.sum" should "support primitive types" in {
    val s = 1 to 10
    runWithData(s: _*)(_.sum).head should equal (55)
    runWithData(s.map(_.toLong): _*)(_.sum).head should equal (55L)
    runWithData(s.map(_.toFloat): _*)(_.sum).head should equal (55F)
    runWithData(s.map(_.toDouble): _*)(_.sum).head should equal (55.0)
  }

  it should "support tuples" in {
    val s = Seq.fill(1000)(Random.nextInt(100)) zip Seq.fill(1000)(Random.nextString(10))

    val output = runWithData(s: _*) { p =>
      p.map(t => (t._1, Set(t._2))).sum
    }.head
    output._1 should equal (s.map(_._1).sum)
    output._2 should equal (s.map(_._2).toSet)
  }

  it should "support HyperLogLog" in {
    val s = Seq.fill(1000)(Random.nextInt(100))
    val expected = s.groupBy(identity).size

    val hll = runWithData(s: _*) { p =>
      val hll = new HyperLogLogMonoid(10)
      p.map(hll.toHLL(_)).sum(hll)
    }.head

    hll.approximateSize.boundsContain(expected) shouldBe true
  }

  it should "support BloomFilter" in {
    val s = Seq.fill(1000)(Random.nextString(10))

    val bf = runWithData(s: _*) { p =>
      val bf = BloomFilter(1000, 0.01)
      p.map(bf.create).sum(bf)
    }.head

    all (s.map(bf.contains(_).isTrue)) shouldBe true
  }

  it should "support QTree" in {
    val q = runWithData(1 to 1000: _*) {
      _.map(QTree(_)).sum(new QTreeSemigroup[Long](10))
    }.head

    boundsContain(q.quantileBounds(0.25), 250) shouldBe true
    boundsContain(q.quantileBounds(0.50), 500) shouldBe true
    boundsContain(q.quantileBounds(0.75), 750) shouldBe true
  }

  it should "support CountMinSketch" in {
    val s = Seq.fill(1000)(Random.nextInt(100))
    val expected = s.groupBy(identity).mapValues(_.size)

    val cms = runWithData(s: _*) { p =>
      import CMSHasherImplicits._
      val cms = CMS.monoid[Int](0.001, 1e-10, 1)
      p.map(cms.create).sum(cms)
    }.head

    all (expected.map(kv => cms.frequency(kv._1).boundsContain(kv._2))) shouldBe true
  }

  it should "support DecayedValue" in {
    val s = (5001 to 6000).map(_.toDouble).zipWithIndex

    val halfLife = 10.0
    val decayFactor = math.exp(math.log(0.5) / halfLife)
    val normalization = halfLife / math.log(2)
    val expected = s.map(_._1).reduce(_ * decayFactor + _) / normalization

    val dv = runWithData(s: _*) {
      _
        .map { case (v, t) => DecayedValue.build(v, t, 10.0) }
        .sum(DecayedValue.monoidWithEpsilon(1e-3))
    }.head

    dv.average(10.0) shouldBe expected +- 1e-3
  }

}
