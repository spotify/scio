/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.{io => beam}
import org.apache.commons.io.FileUtils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class FileSCollectionFunctionsTest extends PipelineSpec {

  private def withTempDir(test: Path => Any): Unit = {
    val dir = Files.createTempDirectory("scio-test-")
    try {
      test(dir)
    } finally {
      FileUtils.deleteDirectory(dir.toFile)
    }
  }

  private def writerTestFiles(dir: Path): Unit = {
    for {
      partition <- Seq("a", "b")
      idx <- Seq(1, 2)
    } yield {
      val p = dir.resolve(partition)
      Files.createDirectories(p)
      val f = p.resolve(s"part-$idx.txt")
      val data = s"$partition$idx"
      Files.write(f, data.getBytes(StandardCharsets.UTF_8))
    }
  }

  "FileSCollection" should "support reading file patterns" in withTempDir { dir =>
    writerTestFiles(dir)
    runWithRealContext() { sc =>
      val actual = sc
        .parallelize(Seq(s"$dir/a/part-*", s"$dir/b/part-*"))
        .readFiles(beam.TextIO.readFiles())
      actual should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
    }
  }

  it should "support reading file patterns with retaining path" in withTempDir { dir =>
    writerTestFiles(dir)
    runWithRealContext() { sc =>
      val actual = sc
        .parallelize(Seq(s"$dir/a/part-*", s"$dir/b/part-*"))
        .readFilesWithPath(
          100L,
          beam.FileIO.ReadMatches.DirectoryTreatment.PROHIBIT,
          beam.Compression.AUTO
        ) { f =>
          new beam.TextSource(
            StaticValueProvider.of(f),
            beam.fs.EmptyMatchTreatment.DISALLOW,
            Array('\n'.toByte),
            0
          )
        }

      actual should containInAnyOrder(
        Seq(
          s"$dir/a/part-1.txt" -> "a1",
          s"$dir/a/part-2.txt" -> "a2",
          s"$dir/b/part-1.txt" -> "b1",
          s"$dir/b/part-2.txt" -> "b2"
        )
      )
    }
  }

}
