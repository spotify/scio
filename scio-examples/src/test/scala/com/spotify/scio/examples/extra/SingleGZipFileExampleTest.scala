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

package com.spotify.scio.examples.extra

import java.io.FileOutputStream
import java.nio.file.Files

import com.spotify.scio.io._
import com.spotify.scio.testing._

class SingleGZipFileExampleTest extends PipelineSpec {

  private val inData = Seq("a b c d e", "a b a b", "")
  private val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "SingleGZipFileExample" should "work" in {
    JobTest[SingleGZipFileExample.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  it should "output compressed data" in {
    val tempDir = Files.createTempDirectory("single-gzip-file-")
    val in = tempDir.resolve("input").toFile
    val inFOS = new FileOutputStream(in.getAbsolutePath)
    inFOS.write(inData.mkString.getBytes)
    inFOS.close()
    val out = tempDir.resolve("output")
    SingleGZipFileExample.main(Array(
      s"--input=${in.getAbsolutePath}",
      s"--output=${out.toFile.getAbsolutePath}"))

    val outPartFile = out.resolve("part-00000-of-00001.txt.deflate").toFile
    outPartFile.exists() shouldBe true
    Files.deleteIfExists(in.toPath)
    Files.deleteIfExists(outPartFile.toPath)
    Files.deleteIfExists(out)
  }

}
