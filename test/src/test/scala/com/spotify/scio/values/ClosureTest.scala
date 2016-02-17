package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class ClosureTest extends PipelineSpec {

  "SCollection" should "support lambdas" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(_ * 10) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support def fn()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      def fn(x: Int): Int = x * 10
      p.map(fn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support val fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val fn = (x: Int) => x * 10
      p.map(fn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  def classFn(x: Int): Int = x * 10

  it should "support class fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(classFn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

  it should "support object fn" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      p.map(ClosureTest.objectFn) should containInAnyOrder (Seq(10, 20, 30))
    }
  }

}

object ClosureTest {
  def objectFn(x: Int): Int = x * 10
}
