/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.transforms

import java.util.concurrent.Semaphore

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.transforms.DoFnWithResource.ResourceType

class SCollectionWithResourceFunctionsTest extends PipelineSpec {

  it should "support filter with resource" in {
    runWithContext { sc =>
      val p =
        sc.parallelize(Seq(1, 2, 3, 4, 5))
          .filterWithResource(new Semaphore(10, true), ResourceType.PER_INSTANCE) { (r, v) =>
            r.acquire()
            r.release()
            v % 2 == 0
          }
      p should containInAnyOrder(Seq(2, 4))
    }
  }

  it should "support flat map with resource" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq("a b c", "d e", "f"))
        .flatMapWithResource(new Semaphore(10, true), ResourceType.PER_INSTANCE) { (r, v) =>
          r.acquire()
          r.release()
          v.split(" ")
        }
      p should containInAnyOrder(Seq("a", "b", "c", "d", "e", "f"))
    }
  }

  it should "support map with resource" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq("1", "2", "3"))
        .mapWithResource(new Semaphore(10, true), ResourceType.PER_INSTANCE) { (r, v) =>
          r.acquire()
          r.release()
          v.toInt
        }
      p should containInAnyOrder(Seq(1, 2, 3))
    }
  }

  it should "support collect with resource" in {
    runWithContext { sc =>
      val records = Seq(
        ("test1", 1),
        ("test2", 2),
        ("test3", 3)
      )
      val p = sc
        .parallelize(records)
        .collectWithResource(new Semaphore(10, true), ResourceType.PER_INSTANCE) {
          case (r, ("test2", v)) =>
            r.acquire()
            r.release()
            2 * v
        }
      p should containSingleValue(4)
    }
  }
}
