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

package com.spotify.scio.io

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class FileStorageTest extends AnyFlatSpec with Matchers {

  def withTempDirectory(test: File => Any): Unit = {
    val dir = Files.createTempDirectory("file-storage-").toFile
    try {
      test(dir)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  "FileStorage.isDone" should "return false on an empty directory" in withTempDirectory { dir =>
    FileStorage(dir.getAbsolutePath, ".txt").isDone() shouldBe false
  }

  it should "return true for non sharded files" in withTempDirectory { dir =>
    new File(dir, "result.txt").createNewFile()
    FileStorage(dir.getAbsolutePath, ".txt").isDone() shouldBe true
  }

  it should "return true on existing files" in withTempDirectory { dir =>
    new File(dir, "part-01-of-02.txt").createNewFile()
    new File(dir, "part-02-of-02.txt").createNewFile()
    FileStorage(dir.getAbsolutePath, ".txt").isDone() shouldBe true
  }

  it should "return false when shards are missing" in withTempDirectory { dir =>
    new File(dir, "pane-1-part-01-of-02.txt").createNewFile()
    new File(dir, "pane-1-part-02-of-02.txt").createNewFile()
    // missing pane-1-part-01-of-02.txt
    new File(dir, "pane-2-part-02-of-02.txt").createNewFile()
    FileStorage(dir.getAbsolutePath, ".txt").isDone() shouldBe false
  }
}
