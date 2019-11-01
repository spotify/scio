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

import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}

class FileStorageTest extends FlatSpec with Matchers {
  "FileStorage.isDone" should "return true on an empty directory" in {
    val dir = Files.createTempDirectory("file-storage-")
    dir.toFile.deleteOnExit()
    FileStorage(dir.toFile.getAbsolutePath).isDone shouldBe true
  }

  it should "return false on non existing files" in {
    val dir = Files.createTempDirectory("file-storage-")
    dir.toFile.deleteOnExit()
    FileStorage(dir.toFile.getAbsolutePath + "/*").isDone shouldBe false
  }

  it should "return true on existing files" in {
    val dir = Files.createTempDirectory("file-storage-")
    val f1 = Files.createTempFile(dir, "part", ".avro")
    val f2 = Files.createTempFile(dir, "part", ".avro")
    dir.toFile.deleteOnExit()
    f1.toFile.deleteOnExit()
    f2.toFile.deleteOnExit()
    FileStorage(dir.toFile.getAbsolutePath + "/*.avro").isDone shouldBe true
  }
}
