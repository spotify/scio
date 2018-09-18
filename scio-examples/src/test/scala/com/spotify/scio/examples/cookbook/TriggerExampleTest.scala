/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.testing._
import org.joda.time.{Duration, Instant}

class TriggerExampleTest extends PipelineSpec {

  "TriggerExample.extractFlowInfo" should "work" in {
    val data = Seq(
      "01/01/2010 00:00:00,1108302,94,E,ML,36,100,29,0.0065,66,9,1,0.001,74.8,1,9,3,0.0028,71,1,9,"
        + "12,0.0099,67.4,1,9,13,0.0121,99.0,1,,,,,0,,,,,0,,,,,0,,,,,0",
      "01/01/2010 00:00:00,"
        + "1100333,5,N,FR,9,0,39,,,9,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,"
    )
    runWithContext { sc =>
      val r = TriggerExample.extractFlowInfo(sc.parallelize(data))
      r should haveSize(1)
      r should containSingleValue(("94", 29))
    }
  }

  "TriggerExample.totalFlow" should "work" in {
    val data = Seq(
      ("01/01/2010 00:00:00,1108302,5,W,ML,36,100,30,0.0065,66,9,1,0.001,"
         + "74.8,1,9,3,0.0028,71,1,9,12,0.0099,87.4,1,9,13,0.0121,99.0,1,,,,,0,,,,,0,,,,,0,,,"
         + ",,0",
       new Instant(60000)),
      ("01/01/2010 00:00:00,1108302,110,E,ML,36,100,40,0.0065,66,9,1,0.001,"
         + "74.8,1,9,3,0.0028,71,1,9,12,0.0099,67.4,1,9,13,0.0121,99.0,1,,,,,0,,,,,0,,,,,0,,,"
         + ",,0",
       new Instant(1)),
      ("01/01/2010 00:00:00,1108302,110,E,ML,36,100,50,0.0065,66,9,1,"
         + "0.001,74.8,1,9,3,0.0028,71,1,9,12,0.0099,97.4,1,9,13,0.0121,50.0,1,,,,,0,,,,,0"
         + ",,,,,0,,,,,0",
       new Instant(1))
    )
    val expected = Seq(
      TriggerExample.Record("default",
                            "5",
                            30,
                            1,
                            "[1970-01-01T00:01:00.000Z..1970-01-01T00:02:00.000Z)",
                            true,
                            true,
                            "ON_TIME",
                            new Instant(1),
                            new Instant(1)),
      TriggerExample.Record("default",
                            "110",
                            90,
                            2,
                            "[1970-01-01T00:00:00.000Z..1970-01-01T00:01:00.000Z)",
                            true,
                            true,
                            "ON_TIME",
                            new Instant(1),
                            new Instant(1))
    )
    runWithContext { sc =>
      val flowInfo = TriggerExample
        .extractFlowInfo(sc.parallelizeTimestamped(data))
        .withFixedWindows(Duration.standardMinutes(1))
      val r = TriggerExample
        .totalFlow(flowInfo, "default")
        .map(_.copy(event_time = new Instant(1), processing_time = new Instant(1)))
      r should containInAnyOrder(expected)
    }
  }

}
