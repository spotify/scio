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
import com.spotify.scio.avro.{Account, AvroIO, GenericRecordTap, SpecificRecordTap}
import com.spotify.scio.io.{TextIO, TextTap}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class SortMergeBucketExampleTest extends AnyFlatSpec with Matchers {
  def withTempFolders(testCode: (File, File, File) => Unit): Unit = {
    val tempFolder = Files.createTempDirectory("smb")
    tempFolder.toFile.deleteOnExit()

    testCode(
      tempFolder.resolve("userData").toFile,
      tempFolder.resolve("accountData").toFile,
      tempFolder.resolve("joinOutput").toFile
    )
  }

  "SortMergeBucketExample" should "join user and account data" in withTempFolders {
    (userDir, accountDir, joinOutputDir) =>
      SortMergeBucketWriteExample.main(
        Array(
          s"--users=$userDir",
          s"--accounts=$accountDir"
        )
      )

      GenericRecordTap(
        path = userDir.getAbsolutePath,
        schema = SortMergeBucketExample.UserDataSchema,
        params = AvroIO.ReadParam(".avro")
      ).value.size shouldBe 500

      SpecificRecordTap[Account](
        path = accountDir.getAbsolutePath,
        params = AvroIO.ReadParam(".avro")
      ).value.size shouldBe 500

      SortMergeBucketJoinExample.main(
        Array(
          s"--users=$userDir",
          s"--accounts=$accountDir",
          s"--output=$joinOutputDir"
        )
      )

      TextTap(
        path = joinOutputDir.getAbsolutePath,
        params = TextIO.ReadParam(suffix = ".txt")
      ).value.size shouldBe 100
  }

  it should "transform user and account data" in withTempFolders {
    (userDir, accountDir, joinOutputDir) =>
      SortMergeBucketWriteExample.main(
        Array(
          s"--users=$userDir",
          s"--accounts=$accountDir"
        )
      )

      SortMergeBucketTransformExample.main(
        Array(
          s"--users=$userDir",
          s"--accounts=$accountDir",
          s"--output=$joinOutputDir"
        )
      )

      SpecificRecordTap[Account](
        joinOutputDir.getAbsolutePath,
        AvroIO.ReadParam(".avro")
      ).value
        .map(account => (account.getId, account.getType.toString))
        .toList should contain theSameElementsAs (0 until 500).map((_, "combinedAmount"))
      ()
  }
}
