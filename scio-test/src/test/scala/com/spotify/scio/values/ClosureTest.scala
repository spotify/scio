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
