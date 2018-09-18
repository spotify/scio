/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio._
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.options.ScioOptions.CheckEnabled
import com.spotify.scio.util.MultiJoin
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest._

class StateTest extends FlatSpec with Matchers {

  type KV[V] = SCollection[(String, V)]

  private def testCogroup[T](f: (KV[Int], KV[Long], KV[String]) => T): Unit = {
    val opts = PipelineOptionsFactory.as(classOf[ScioOptions])
    opts.setChainedCogroups(CheckEnabled.ERROR)
    val sc = ScioContext(opts)
    val a = sc.parallelize(Seq.empty[(String, Int)])
    val b = sc.parallelize(Seq.empty[(String, Long)])
    val c = sc.parallelize(Seq.empty[(String, String)])
    f(a, b, c)
    sc.close()
  }

  "SCollection.State" should "pass MultiJoin" in {
    testCogroup {
      case (a, b, c) =>
        noException shouldBe thrownBy { MultiJoin(a, b, c) }
    }
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail chained cogroups" in {
    testCogroup {
      case (a, b, c) =>
        an[RuntimeException] shouldBe thrownBy { a.cogroup(b).cogroup(c) }
    }
  }

  it should "fail chained joins" in {
    testCogroup {
      case (a, b, c) =>
        an[RuntimeException] shouldBe thrownBy { a.join(b).join(c) }
    }
  }

  it should "fail chained left outer joins" in {
    testCogroup {
      case (a, b, c) =>
        an[RuntimeException] shouldBe thrownBy {
          a.leftOuterJoin(b).leftOuterJoin(c)
        }
    }
  }

  it should "fail chained right outer joins" in {
    testCogroup {
      case (a, b, c) =>
        an[RuntimeException] shouldBe thrownBy {
          a.rightOuterJoin(b).rightOuterJoin(c)
        }
    }
  }

  it should "fail chained full outer joins" in {
    testCogroup {
      case (a, b, c) =>
        an[RuntimeException] shouldBe thrownBy {
          a.fullOuterJoin(b).fullOuterJoin(c)
        }
    }
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
