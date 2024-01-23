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

import com.spotify.scio.ContextAndArgs

import java.io.File
import java.nio.file.Files
import com.spotify.scio.avro.Account
import com.spotify.scio.examples.extra.SortMergeBucketJoinExample.AccountProjection
import com.spotify.scio.examples.extra.SortMergeBucketTransformExample.CombinedAccount
import com.spotify.scio.io.TextIO
import com.spotify.scio.smb.SmbIO
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.smb.BucketMetadata
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.io.{FileSystems, LocalResources}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier

import java.nio.channels.Channels
import java.util.UUID

class SortMergeBucketExampleTest extends PipelineSpec {
  private def withTempFolders(testCode: Supplier[File] => Unit): Unit = {
    val tempFolder = Files.createTempDirectory("smb")
    tempFolder.toFile.deleteOnExit()

    testCode(() => tempFolder.resolve(UUID.randomUUID().toString).toFile)
  }

  "SortMergeBucketWriteExample" should "work with JobTest" in {
    JobTest[SortMergeBucketWriteExample.type]
      .args("--users=gs://users", "--accounts=gs://accounts")
      .output(SmbIO[Int, GenericRecord]("gs://users", _.get("userId").asInstanceOf[Int]))(
        _ should haveSize(500)
      )
      .output(SmbIO[Int, AccountProjection]("gs://accounts", _.id))(_ should haveSize(500))
      .run()
  }

  it should "work with Taps" in withTempFolders { tmpDirSupplier =>
    val (userDir, accountDir) = (tmpDirSupplier.get(), tmpDirSupplier.get())

    val (sc, args) = ContextAndArgs(Array(s"--users=$userDir", s"--accounts=$accountDir"))
    val (userTap, accountTap) = SortMergeBucketWriteExample.pipeline(sc, args)
    val writeResult = sc.run().waitUntilDone()

    // Open Taps and assert on Iterator output
    userTap
      .get(writeResult)
      .value
      .map(_.get("userId").asInstanceOf[Integer])
      .toSeq should contain theSameElementsAs (0 until 500).map(i => i % 100)

    accountTap
      .get(writeResult)
      .value
      .map(_.getId)
      .toSeq should contain theSameElementsAs (250 until 750).map(i => i % 100)

    // Inspect metadata on real written files
    def getMetadataPath(smbDir: File): ResourceId =
      LocalResources.fromFile(smbDir.toPath.resolve("metadata.json").toFile, false)

    BucketMetadata
      .from(Channels.newInputStream(FileSystems.open(getMetadataPath(userDir))))
      .getNumBuckets shouldBe 2
    BucketMetadata
      .from(Channels.newInputStream(FileSystems.open(getMetadataPath(accountDir))))
      .getNumBuckets shouldBe 1
  }

  "SortMergeBucketJoinExample" should "work with JobTest" in {
    val userInput = (0 until 200)
      .map(i => SortMergeBucketExample.user(i, i % 100))
    val accountInput = (25 until 225)
      .map(i => AccountProjection(i, i * 10.0d))

    val expectedOutput = (0 until 200)
      .flatMap { userId =>
        for {
          user <- userInput.filter(_.get("userId").asInstanceOf[Int] == userId)
          account <- accountInput.filter(_.id == userId)
        } yield (user.get("age").asInstanceOf[Integer], account.amount)
      }
      .groupBy(_._1)
      .map { case (age, grped) =>
        val amounts = grped.map(_._2)
        (age, amounts.sum / amounts.size) // Compute average account value per age
      }

    JobTest[SortMergeBucketJoinExample.type]
      .args("--users=gs://users", "--accounts=gs://accounts", "--output=gs://output")
      .input(
        SmbIO[Integer, GenericRecord]("gs://users", _.get("userId").asInstanceOf[Integer]),
        userInput
      )
      .input(SmbIO[Int, AccountProjection]("gs://accounts", _.id), accountInput)
      .output(TextIO("gs://output"))(_ should containInAnyOrder(expectedOutput.map(_.toString)))
      .run()
  }

  it should "work with Taps" in withTempFolders { tmpDirSupplier =>
    val (userDir, accountDir) = (tmpDirSupplier.get(), tmpDirSupplier.get())

    // Write local data
    {
      val (writeSc, writeArgs) =
        ContextAndArgs(Array(s"--users=$userDir", s"--accounts=$accountDir"))
      SortMergeBucketWriteExample.pipeline(writeSc, writeArgs)
      writeSc.run().waitUntilDone()
    }

    // Run SortMergeBucketJoinExample and assert on tapped output
    val joinOutput = tmpDirSupplier.get()

    val (sc, args) =
      ContextAndArgs(Array(s"--users=$userDir", s"--accounts=$accountDir", s"--output=$joinOutput"))
    val tap = SortMergeBucketJoinExample.pipeline(sc, args)
    val result = sc.run().waitUntilDone()

    val joinedSmbData = tap.get(result).value
    joinedSmbData should have size 50
  }

  "SortMergeBucketTransformExample" should "work with JobTest" in {
    val userInput = (0 until 100)
      .map(i => SortMergeBucketExample.user(i, i % 100))
    val accountInput = (100 until 300)
      .map(i => AccountProjection(i, i * 10.0d))

    val expectedOutput = (0 until 200)
      .flatMap { userId =>
        for {
          user <- userInput.filter(_.get("userId").asInstanceOf[Integer] == userId)
          account <- accountInput.filter(_.id == userId)
        } yield (
          (userId.asInstanceOf[Integer], user.get("age").asInstanceOf[Integer]),
          account.amount
        )
      }
      .groupBy(_._1)
      .map { case ((userId, age), grped) => CombinedAccount(userId, age, grped.map(_._2).sum) }

    JobTest[SortMergeBucketTransformExample.type]
      .args("--users=gs://users", "--accounts=gs://accounts", "--output=gs://output")
      .input(
        SmbIO[Integer, GenericRecord]("gs://users", _.get("userId").asInstanceOf[Integer]),
        userInput
      )
      .input(SmbIO[Integer, AccountProjection]("gs://accounts", _.id), accountInput)
      .output(SmbIO[Integer, SortMergeBucketTransformExample.CombinedAccount]("gs://output", _.id))(
        _ should containInAnyOrder(expectedOutput)
      )
      .run()
  }

  it should "work with Taps" in withTempFolders { tmpDirSupplier =>
    val (userDir, accountDir) = (tmpDirSupplier.get(), tmpDirSupplier.get())

    // Write local data
    {
      val (writeSc, writeArgs) =
        ContextAndArgs(Array(s"--users=$userDir", s"--accounts=$accountDir"))
      SortMergeBucketWriteExample.pipeline(writeSc, writeArgs)
      writeSc.run().waitUntilDone()
    }

    // Run SortMergeBucketTransformExample and assert on tapped output
    val transformOutput = tmpDirSupplier.get()

    val (sc, args) = ContextAndArgs(
      Array(s"--users=$userDir", s"--accounts=$accountDir", s"--output=${transformOutput}")
    )
    val tap = SortMergeBucketTransformExample.pipeline(sc, args)
    val result = sc.run().waitUntilDone()

    val transformedSmbData = tap.get(result).value
    transformedSmbData should have size 250
    // Assert that Parquet FilterPredicate was applied
    assert(transformedSmbData.forall(r => r.age < 50))
  }
}
