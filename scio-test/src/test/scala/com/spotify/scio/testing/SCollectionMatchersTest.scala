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

package com.spotify.scio.testing

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException

// scalastyle:off no.whitespace.before.left.bracket
class SCollectionMatchersTest extends PipelineSpec {

  "SCollectionMatch" should "support containInAnyOrder" in {
    runWithContext {
      _.parallelize(1 to 100) should containInAnyOrder (1 to 100)
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 200) should containInAnyOrder (1 to 100) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should containInAnyOrder (1 to 200) }
    }
  }

  it should "support containSingleValue" in {
    runWithContext { _.parallelize(Seq(1)) should containSingleValue (1) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) should containSingleValue (10) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should containSingleValue (1) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) should containSingleValue (1) }
    }
  }

  it should "support beEmpty" in {
    runWithContext { _.parallelize(Seq.empty[Int]) should beEmpty }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should beEmpty }
    }
  }

  it should "support haveSize" in {
    runWithContext { _.parallelize(Seq.empty[Int]) should haveSize (0) }
    runWithContext { _.parallelize(Seq(1)) should haveSize (1) }
    runWithContext { _.parallelize(1 to 10) should haveSize (10) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) should haveSize(1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) should haveSize(0) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should haveSize (20) }
    }
  }

  it should "support equalMapOf" in {
    val s = Seq("a" -> 1, "b" -> 2, "c" -> 3)
    runWithContext { sc =>
      sc.parallelize(s) should equalMapOf (s.toMap)
      sc.parallelize(Seq.empty[(String, Int)]) should equalMapOf (Map.empty[String, Int])
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf ((s :+ "d" -> 4).toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s :+ "d" -> 4) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (s.tail.toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s.tail) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (s.toMap + ("a" -> 10)) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s.tail :+ ("a" -> 10)) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (Map.empty[String, Int]) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[(String, Int)]) should equalMapOf (s.toMap) }
    }
  }

  it should "support notEqualMapOf" in {
    val s = Seq("a" -> 1, "b" -> 2, "c" -> 3)
    runWithContext { sc =>
      sc.parallelize(s) should notEqualMapOf (s.toMap + ("d" -> 4))
      sc.parallelize(Seq.empty[(String, Int)]) should notEqualMapOf (Map("a" -> 1))
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should notEqualMapOf (s.toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[(String, Int)]) should notEqualMapOf (Map.empty[String, Int])
      }
    }
  }

  it should "support satisfy" in {
    runWithContext { _.parallelize(1 to 100) should satisfy[Int] (_.sum == 5050) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should satisfy[Int] (_.sum == 100) }
    }
  }

  it should "support forAll" in {
    runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 0)}

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 10)}
    }
  }

  it should "support exist" in {
    runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 99)}

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 100)}
    }
  }

}
// scalastyle:on no.whitespace.before.left.bracket