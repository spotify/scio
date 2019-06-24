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

import com.spotify.scio.io._
import com.spotify.scio.testing._

class TemplateExampleTest extends PipelineSpec {

  val inData = (1 to 10).map(_.toString)

  "TemplateExample" should "write to a Pubsub topic" in {
    JobTest[TemplateExample.type]
      .args(
        "--inputSubscription=projects/project/subscriptions/subscription",
        "--outputTopic=projects/project/topics/topic"
      )
      .input(CustomIO[String]("input"), inData)
      .output(CustomIO[String]("output")) { coll =>
        coll should containInAnyOrder(inData)
      }
      .run()
  }

}
