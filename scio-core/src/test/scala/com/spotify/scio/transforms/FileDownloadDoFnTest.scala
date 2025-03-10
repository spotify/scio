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

package com.spotify.scio.transforms

import java.nio.file.{Files, Path}
import com.spotify.scio.testing._
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.{Files => GFiles}
import org.joda.time.Instant

import scala.jdk.CollectionConverters._

class FileDownloadDoFnTest extends PipelineSpec {
  "FileDownloadDoFn" should "work" in {
    val tmpDir = Files.createTempDirectory("filedofn-")
    val files = createFiles(tmpDir, 100)
    runWithContext { sc =>
      val p = sc.parallelize(files.map(_.toUri)).flatMapFile(fn)

      val content = p.keys
      val paths = p.values.distinct

      val expected = (1 to 100).map(_.toString)
      content should containInAnyOrder(expected)
      paths should forAll { f: Path => !Files.exists(f) }
    }
    files.foreach(Files.delete)
    Files.delete(tmpDir)
  }

  it should "support batch" in {
    val tmpDir = Files.createTempDirectory("filedofn-")
    val files = createFiles(tmpDir, 100)
    runWithContext { sc =>
      // try to use a single bundle so we can check
      // elements flushed in processElement as well as
      // elements flushed in finishBundle
      val p = sc
        .parallelize(Seq(files.map(_.toUri).zipWithIndex))
        .flatten
        .timestampBy { case (_, i) => new Instant(i + 1) }
        .keys
        .flatMapFile(fn, 10, false)
        .withTimestamp

      val contentAndTimestamp = p.map { case ((i, _), ts) => (i, ts.getMillis) }
      val paths = p.map { case ((_, f), _) => f }.distinct

      val expected = (1L to 100L).map(i => (i.toString, i))
      contentAndTimestamp should containInAnyOrder(expected)
      paths should forAll { f: Path => !Files.exists(f) }
    }
    files.foreach(Files.delete)
    Files.delete(tmpDir)
  }

  it should "support keeping downloaded files" in {
    val tmpDir = Files.createTempDirectory("filedofn-")
    val files = createFiles(tmpDir, 100)
    runWithContext { sc =>
      val p = sc.parallelize(files.map(_.toUri)).flatMapFile(fn, 10, true)
      p.keys should containInAnyOrder((1 to 100).map(_.toString))
      p.values.distinct should forAll { f: Path =>
        val r = Files.exists(f)
        if (r) {
          Files.delete(f)
        }
        r
      }
    }
    files.foreach(Files.delete)
    Files.delete(tmpDir)
  }

  private def createFiles(dir: Path, n: Int): Seq[Path] =
    (1 to n).map { i =>
      val file = dir.resolve("part-%05d-of-%05d.txt".format(i, n))
      GFiles.asCharSink(file.toFile, Charsets.UTF_8).write(i.toString)
      file
    }

  private def fn(input: Path) =
    Files.readAllLines(input).asScala.map((_, input))
}
