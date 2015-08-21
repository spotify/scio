package com.spotify.scio.values

import com.spotify.scio.testing.PipelineTest

class ClosureTest extends PipelineTest {

  "SCollection" should "support lambdas" in {
    runWithContext { context =>
      val p = context.parallelize(1, 2, 3)
      p.map(_ * 10).internal should containInAnyOrder (10, 20, 30)
    }
  }

  it should "support def fn()" in {
    runWithContext { context =>
      val p = context.parallelize(1, 2, 3)
      def fn(x: Int) = x * 10
      p.map(fn).internal should containInAnyOrder (10, 20, 30)
    }
  }

  it should "support val fn" in {
    runWithContext { context =>
      val p = context.parallelize(1, 2, 3)
      val fn = (x: Int) => x * 10
      p.map(fn).internal should containInAnyOrder (10, 20, 30)
    }
  }

  def classFn(x: Int) = x * 10

  it should "support class fn" in {
    runWithContext { context =>
      val p = context.parallelize(1, 2, 3)
      p.map(classFn).internal should containInAnyOrder (10, 20, 30)
    }
  }

  it should "support object fn" in {
    runWithContext { context =>
      val p = context.parallelize(1, 2, 3)
      p.map(ClosureTest.objectFn).internal should containInAnyOrder (10, 20, 30)
    }
  }

}

object ClosureTest {
  def objectFn(x: Int) = x * 10
}
