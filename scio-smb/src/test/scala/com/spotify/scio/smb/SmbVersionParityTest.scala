/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.smb

import com.spotify.scio.avro.{Account, AccountStatus}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.{
  AvroSortedBucketIO,
  ParquetAvroSortedBucketIO,
  SortedBucketIO
}
import org.apache.beam.sdk.values.TupleTag

import java.nio.file.Files

class SmbVersionParityTest extends PipelineSpec {
  private def testRoundtrip(
    write: SortedBucketIO.Write[CharSequence, _, Account],
    read: SortedBucketIO.Read[Account]
  ): Unit = {
    val accounts = (1 to 10).map { i =>
      Account
        .newBuilder()
        .setId(i)
        .setName(i.toString)
        .setAmount(i.toDouble)
        .setType(s"type$i")
        .setAccountStatus(AccountStatus.Active)
        .build()
    }

    {
      val sc = ScioContext()
      sc.parallelize(accounts)
        .saveAsSortedBucket(write)
      sc.run()
    }

    // Read data
    val sc = ScioContext()
    val tap = sc
      .sortMergeGroupByKey(classOf[String], read)
      .materialize
    tap
      .get(sc.run().waitUntilDone())
      .flatMap(_._2)
      .value
      .toSeq should contain theSameElementsAs accounts
  }

  "SortedBucketSource" should "be able to read CharSequence-keyed Avro sources written before 0.14" in {
    val output = Files.createTempDirectory("smb-version-test-avro").toFile
    output.deleteOnExit()

    testRoundtrip(
      AvroSortedBucketIO
        .write(classOf[CharSequence], "name", classOf[Account])
        .to(output.getAbsolutePath)
        .withNumBuckets(1)
        .withNumShards(1),
      AvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(output.getAbsolutePath)
    )
  }

  it should "be able to read CharSequence-keyed Parquet sources written before 0.14" in {
    val output = Files.createTempDirectory("smb-version-test-parquet").toFile
    output.deleteOnExit()

    testRoundtrip(
      ParquetAvroSortedBucketIO
        .write(classOf[CharSequence], "name", classOf[Account])
        .to(output.getAbsolutePath)
        .withNumBuckets(1)
        .withNumShards(1),
      ParquetAvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(output.getAbsolutePath)
    )
  }
}
