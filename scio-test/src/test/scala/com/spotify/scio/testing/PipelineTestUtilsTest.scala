/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.testing

import com.spotify.scio.ScioMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class PipelineTestUtilsTest
    extends AnyFlatSpec
    with Matchers
    with SCollectionMatchers
    with PipelineTestUtils {

  "runWithContext" should "fail input with message" in {
    val msg = "requirement failed: Missing test data. Are you reading input outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.textFile("in.txt"))
    } should have message msg
  }

  it should "fail output with message" in {
    val msg = "requirement failed: Missing test data. Are you writing output outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.parallelize(1 to 10).materialize)
    } should have message msg
  }

  it should "fail dist cache with message" in {
    val msg = "requirement failed: Missing test data. Are you using dist cache outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.distCache("in.txt")(f => Source.fromFile(f).getLines().toSeq))
    } should have message msg
  }

  "runWithData" should "take the input and materialize the output" in {
    val output = runWithData(Seq(1, 2, 3))(_.map(_ * 2))
    output should contain theSameElementsAs Seq(2, 4, 6)
  }

  "runWithLocalOutput" should "return the scio result along with the materialized output" in {
    val c = ScioMetrics.counter("counter")
    val (result, output) = runWithLocalOutput { sc =>
      sc.parallelize(Seq(1, 2, 3)).tap(_ => c.inc()).map(_ * 2)
    }
    output should contain theSameElementsAs Seq(2, 4, 6)
    result.counter(c).committed shouldBe Some(3)
  }

  "runWithOverride" should "override named transforms from the pipeline" in {
    runWithOverrides(
      TransformOverride.of("multiply", (v: Int) => v * v),
      TransformOverride.ofIter("append", (v: Int) => Seq(v + "c", v + "d"))
    ) { sc =>
      val result = sc
        .parallelize(Seq(1, 2, 3))
        .withName("multiply")
        .map(_ * 2)
        .withName("append")
        .flatMap(v => Seq(v + "a", v + "b"))

      result should containInAnyOrder(Seq("1c", "1d", "4c", "4d", "9c", "9d"))
    }
  }

}
