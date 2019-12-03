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

import java.io.File
import java.nio.file.Files

import com.spotify.scio.avro.{Account, GenericRecordTap, SpecificRecordTap}
import com.spotify.scio.io.TextTap
import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}

class SortMergeBucketExampleTest extends FlatSpec with Matchers {

  def withTempFolders(testCode: (File, File, File) => Unit): Unit = {
    val tempFolder = Files.createTempDirectory("smb")
    try {
      testCode(
        tempFolder.resolve("userData").toFile,
        tempFolder.resolve("accountData").toFile,
        tempFolder.resolve("joinOutput").toFile
      )
    } finally {
      FileUtils.deleteDirectory(tempFolder.toFile)
    }
  }

  "SortMergeBucketExample" should "join user and account data" in withTempFolders {
    (userDir, accountDir, joinOutputDir) => {
      SortMergeBucketWriteExample.main(
        Array(
          s"--outputL=$userDir",
          s"--outputR=$accountDir"
        )
      )

      GenericRecordTap(
        s"$userDir/*.avro",
        SortMergeBucketExample.UserDataSchema
      ).value.size shouldBe 500

      SpecificRecordTap[Account](s"$accountDir/*.avro").value.size shouldBe 500

      SortMergeBucketJoinExample.main(
        Array(
          s"--inputL=$userDir",
          s"--inputR=$accountDir",
          s"--output=$joinOutputDir"
        )
      )

      TextTap(s"$joinOutputDir/*.txt").value.size shouldBe 250
    }
  }
}
