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
  private def testReadRoundtrip(
    writes: Seq[SortedBucketIO.Write[_ <: CharSequence, _, Account]],
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
      val records = sc.parallelize(accounts)
      writes.foreach(records.saveAsSortedBucket(_))
      sc.run()
      ()
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
      .toSeq should contain theSameElementsAs writes.flatMap(_ => accounts)
  }

  private def testTransformRoundtrip(
    writes: Seq[SortedBucketIO.Write[_ <: CharSequence, Void, Account]],
    read: SortedBucketIO.Read[Account],
    transform: SortedBucketIO.TransformOutput[String, Void, Account]
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
      val records = sc.parallelize(accounts)
      writes.foreach(records.saveAsSortedBucket(_))
      sc.run()
      ()
    }

    // Read data
    val sc = ScioContext()
    val tap = sc
      .sortMergeTransform(classOf[String], read)
      .to(transform)
      .via { case (_, records, outputCollector) =>
        records.foreach(outputCollector.accept(_))
      }

    tap
      .get(sc.run().waitUntilDone())
      .value
      .toSeq should contain theSameElementsAs writes.flatMap(_ => accounts)
  }

  "SortedBucketSource" should "be able to read mixed CharSequence and String-keyed Avro sources" in {
    val tmpDir = Files.createTempDirectory("smb-version-test-mixed-avro-read").toFile
    tmpDir.deleteOnExit()

    val partition1Output = tmpDir.toPath.resolve("partition1")
    val partition2Output = tmpDir.toPath.resolve("partition2")

    testReadRoundtrip(
      Seq(
        AvroSortedBucketIO
          .write(classOf[CharSequence], "name", classOf[Account])
          .to(partition1Output.toString)
          .withNumBuckets(1)
          .withNumShards(1),
        AvroSortedBucketIO
          .write(classOf[String], "name", classOf[Account])
          .to(partition2Output.toString)
          .withNumBuckets(1)
          .withNumShards(1)
      ),
      AvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(partition1Output.toString, partition2Output.toString)
    )
  }

  it should "be able to transform mixed CharSequence- and String-keyed Avro sources written before 0.14" in {
    val tmpDir = Files.createTempDirectory("smb-version-test-avro-tfx").toFile
    tmpDir.deleteOnExit()

    val partition1Output = tmpDir.toPath.resolve("partition1")
    val partition2Output = tmpDir.toPath.resolve("partition2")
    val tfxOutput = tmpDir.toPath.resolve("tfx")

    testTransformRoundtrip(
      Seq(
        AvroSortedBucketIO
          .write(classOf[CharSequence], "name", classOf[Account])
          .to(partition1Output.toString)
          .withNumBuckets(1)
          .withNumShards(1),
        AvroSortedBucketIO
          .write(classOf[String], "name", classOf[Account])
          .to(partition2Output.toString)
          .withNumBuckets(1)
          .withNumShards(1)
      ),
      AvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(partition1Output.toString, partition2Output.toString),
      AvroSortedBucketIO
        .transformOutput(classOf[String], "name", classOf[Account])
        .to(tfxOutput.toString)
    )
  }

  it should "be able to read mixed CharSequence- and String-keyed-keyed Parquet sources written before 0.14" in {
    val tmpDir = Files.createTempDirectory("smb-version-test-mixed-parquet-read").toFile
    tmpDir.deleteOnExit()

    val partition1Output = tmpDir.toPath.resolve("partition1")
    val partition2Output = tmpDir.toPath.resolve("partition2")

    testReadRoundtrip(
      Seq(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "name", classOf[Account])
          .to(partition1Output.toString)
          .withNumBuckets(1)
          .withNumShards(1),
        ParquetAvroSortedBucketIO
          .write(classOf[String], "name", classOf[Account])
          .to(partition2Output.toString)
          .withNumBuckets(1)
          .withNumShards(1)
      ),
      ParquetAvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(partition1Output.toString, partition2Output.toString)
    )
  }

  it should "be able to transform mixed CharSequence- and String-keyed Parquet sources written before 0.14" in {
    val tmpDir = Files.createTempDirectory("smb-version-test-parquet-tfx").toFile
    tmpDir.deleteOnExit()

    val partition1Output = tmpDir.toPath.resolve("partition1")
    val partition2Output = tmpDir.toPath.resolve("partition2")
    val tfxOutput = tmpDir.toPath.resolve("tfx")

    testTransformRoundtrip(
      Seq(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "name", classOf[Account])
          .to(partition1Output.toString)
          .withNumBuckets(1)
          .withNumShards(1),
        ParquetAvroSortedBucketIO
          .write(classOf[String], "name", classOf[Account])
          .to(partition2Output.toString)
          .withNumBuckets(1)
          .withNumShards(1)
      ),
      ParquetAvroSortedBucketIO
        .read(new TupleTag[Account], classOf[Account])
        .from(partition1Output.toString, partition2Output.toString),
      ParquetAvroSortedBucketIO
        .transformOutput(classOf[String], "name", classOf[Account])
        .to(tfxOutput.toString)
    )
  }
}
