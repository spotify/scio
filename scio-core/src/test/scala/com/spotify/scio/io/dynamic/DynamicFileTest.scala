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

package com.spotify.scio.io.dynamic

import com.spotify.scio._

import java.nio.file.Files
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.{Duration, Instant}

import java.io.File
import scala.jdk.CollectionConverters._

class DynamicFileTest extends PipelineSpec with TapSpec {

  private def partitionIntegers(s: String): String =
    partitionIntegers(s.toInt)

  private def partitionIntegers(n: Int): String =
    if (n % 2 == 0) "even" else "odd"

  private def verifyOutput(path: File, expected: String*): Unit = {
    val p = path.toPath
    val actual = Files
      .list(p)
      .iterator()
      .asScala
      .filterNot(_.toFile.getName.startsWith("."))
      .toSet
    actual shouldBe expected.map(p.resolve).toSet
  }

  "Dynamic File" should "support text files" in withTempDir { dir =>
    val sc1 = ScioContext()
    sc1
      .parallelize(1 to 10)
      .saveAsDynamicTextFile(dir.getAbsolutePath)(partitionIntegers)
    sc1.run()
    verifyOutput(dir, "even", "odd")

    val sc2 = ScioContext()
    val even = sc2.textFile(s"$dir/even/*.txt")
    val odd = sc2.textFile(s"$dir/odd/*.txt")
    val (expectedEven, expectedOdd) = (1 to 10).partition(_ % 2 == 0)
    even should containInAnyOrder(expectedEven.map(_.toString))
    odd should containInAnyOrder(expectedOdd.map(_.toString))
    sc2.run()
  }

  it should "support text files with windowing" in withTempDir { dir =>
    val options = PipelineOptionsFactory.fromArgs("--streaming=true").create()
    val sc1 = ScioContext(options)
    sc1
      .parallelize(1 to 10)
      // Explicit optional arguments `Duration.Zero` and `WindowOptions()` as a workaround for the
      // mysterious "Could not find proxy for val sc1" compiler error
      .timestampBy(x => new Instant(x * 60000L), Duration.ZERO)
      .withFixedWindows(Duration.standardMinutes(1), Duration.ZERO, WindowOptions())
      .saveAsDynamicTextFile(dir.getAbsolutePath, 1)(partitionIntegers)
    sc1.run()
    verifyOutput(dir, "even", "odd")
    Files.list(dir.toPath.resolve("even")).iterator().asScala.size shouldBe 5
    Files.list(dir.toPath.resolve("odd")).iterator().asScala.size shouldBe 5

    val sc2 = ScioContext()
    val even = sc2.textFile(s"$dir/even/*.txt")
    val odd = sc2.textFile(s"$dir/odd/*.txt")
    val (expectedEven, expectedOdd) = (1 to 10).partition(_ % 2 == 0)
    even should containInAnyOrder(expectedEven.map(_.toString))
    odd should containInAnyOrder(expectedOdd.map(_.toString))
    (1 to 10).foreach { x =>
      val p = partitionIntegers(x % 2)
      val t1 = new Instant(x * 60000L)
      val t2 = t1.plus(60000L)
      val lines = sc2.textFile(s"$dir/$p/part-$t1-$t2-*.txt")
      lines should containSingleValue(x.toString)
    }
    sc2.run()
  }

  it should "support text files with optional header and footer" in withTempDir { dir =>
    val sc1 = ScioContext()
    sc1
      .parallelize(1 to 10)
      .saveAsDynamicTextFile(
        path = dir.getAbsolutePath,
        numShards = 1,
        header = Some("header"),
        footer = Some("footer")
      )(partitionIntegers)
    sc1.run()
    verifyOutput(dir, "even", "odd")

    val sc2 = ScioContext()
    val even = sc2.textFile(s"$dir/even/*.txt")
    val odd = sc2.textFile(s"$dir/odd/*.txt")
    val expectedMetadata = Seq("header", "footer")
    val (expectedEven, expectedOdd) = (1 to 10).partition(_ % 2 == 0)
    even should containInAnyOrder(expectedMetadata ++ expectedEven.map(_.toString))
    odd should containInAnyOrder(expectedMetadata ++ expectedOdd.map(_.toString))
    sc2.run()
  }
}
