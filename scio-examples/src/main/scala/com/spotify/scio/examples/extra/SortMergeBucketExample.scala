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

// Example: Sort Merge Bucket write and join
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketWriteExample
// --users=[OUTPUT] --accounts=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketJoinExample
// --users=[INPUT] --accounts=[INPUT] --output=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketTransformExample
// --users=[INPUT] --accounts=[INPUT] --output=[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.values.SCollection
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.{
  ParquetAvroSortedBucketIO,
  ParquetTypeSortedBucketIO,
  TargetParallelism
}
import org.apache.beam.sdk.values.TupleTag
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.ParquetOutputFormat

import scala.util.Random

object SortMergeBucketExample {
  lazy val UserDataSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |    "name": "UserData",
      |    "namespace": "com.spotify.examples.extra",
      |    "type": "record",
      |    "fields": [
      |        {
      |            "name": "userId",
      |            "type": "int"
      |        },
      |        {
      |          "name": "age", "type": "int"
      |        }
      |    ]}
      |""".stripMargin
  )

  def user(id: Int, age: Int): GenericRecord =
    new GenericRecordBuilder(UserDataSchema)
      .set("userId", id)
      .set("age", age)
      .build()
}

object SortMergeBucketWriteExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    pipeline(sc, args)
    sc
  }

  def pipeline(sc: ScioContext, args: Args): (ClosedTap[GenericRecord], ClosedTap[Account]) = {
    val userWriteTap = sc
      .parallelize(0 until 500)
      .map(i => SortMergeBucketExample.user(i % 100, i % 100))
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[Integer], "userId", SortMergeBucketExample.UserDataSchema)
          .to(args("users"))
          .withTempDirectory(sc.options.getTempLocation)
          .withHashType(HashType.MURMUR3_32)
          .withFilenamePrefix("example-prefix")
          .withNumBuckets(2)
          .withNumShards(1)
      )

    // #SortMergeBucketExample_sink
    val accountWriteTap = sc
      .parallelize(250 until 750)
      .map { i =>
        Account
          .newBuilder()
          .setId(i % 100)
          .setName(s"name$i")
          .setType(s"type${i % 5}")
          .setAmount(Random.nextDouble() * 1000)
          .build()
      }
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write[Integer, Account](classOf[Integer], "id", classOf[Account])
          .to(args("accounts"))
          .withSorterMemoryMb(128)
          .withTempDirectory(sc.options.getTempLocation)
          .withConfiguration(
            ParquetConfiguration.of(ParquetOutputFormat.BLOCK_SIZE -> 512 * 1024 * 1024)
          )
          .withHashType(HashType.MURMUR3_32)
          .withFilenamePrefix("part") // Default is "bucket"
          .withNumBuckets(1)
          .withNumShards(1)
      )
    // #SortMergeBucketExample_sink

    (userWriteTap, accountWriteTap)
  }

  def secondaryKeyExample(
    args: Args,
    in: SCollection[Account]
  ): Unit = {
    in
      // #SortMergeBucketExample_sink_secondary
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write[Integer, String, Account](
            // primary key class and field
            classOf[Integer],
            "id",
            // secondary key class and field
            classOf[String],
            "type",
            classOf[Account]
          )
          .to(args("accounts"))
      )
    // #SortMergeBucketExample_sink_secondary
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
    ()
  }
}

object SortMergeBucketJoinExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  case class AccountProjection(id: Int, amount: Double)

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    pipeline(sc, args)
    sc
  }

  def pipeline(sc: ScioContext, args: Args): ClosedTap[String] = {
    // #SortMergeBucketExample_join
    sc.sortMergeJoin(
      classOf[Integer],
      ParquetAvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("users"), SortMergeBucketExample.UserDataSchema)
        .withProjection(
          SchemaBuilder
            .record("UserProjection")
            .fields
            .requiredInt("userId")
            .requiredInt("age")
            .endRecord
        )
        // Filter at the Parquet IO level to users under 50
        // Filtering at the IO level whenever possible, as it reduces total bytes read
        .withFilterPredicate(FilterApi.lt(FilterApi.intColumn("age"), Int.box(50)))
        // Filter at the SMB Cogrouping level to a single record per user
        // Filter at the Cogroup level if your filter depends on the materializing key group
        .withPredicate((xs, _) => xs.size() == 0)
        .from(args("users")),
      ParquetTypeSortedBucketIO
        .read(new TupleTag[AccountProjection]("accounts"))
        .from(args("accounts")),
      TargetParallelism.max()
    ).map { case (_, (userData, account)) =>
      (userData.get("age").asInstanceOf[Int], account.amount)
    }.groupByKey
      .mapValues(amounts => amounts.sum / amounts.size)
      .saveAsTextFile(args("output"))
    // #SortMergeBucketExample_join
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
    ()
  }
}

object SortMergeBucketTransformExample {
  import com.spotify.scio.smb._

  // ParquetTypeSortedBucketIO supports case class projections for reading and writing
  case class AccountProjection(id: Int, amount: Double)
  case class CombinedAccount(id: Int, age: Int, totalValue: Double)

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    pipeline(sc, args)
    sc
  }

  def pipeline(sc: ScioContext, args: Args): ClosedTap[CombinedAccount] = {
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(
      SortMergeBucketExample.UserDataSchema
    )

    // #SortMergeBucketExample_transform
    sc.sortMergeTransform(
      classOf[Integer],
      ParquetAvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("users"), SortMergeBucketExample.UserDataSchema)
        // Filter at the Parquet IO level to users under 50
        .withFilterPredicate(FilterApi.lt(FilterApi.intColumn("age"), Int.box(50)))
        .from(args("users")),
      ParquetTypeSortedBucketIO
        .read(new TupleTag[AccountProjection]("accounts"))
        .from(args("accounts")),
      TargetParallelism.auto()
    ).to(
      ParquetTypeSortedBucketIO
        .transformOutput[Integer, CombinedAccount]("id")
        .to(args("output"))
    ).via { case (key, (users, accounts), outputCollector) =>
      val sum = accounts.map(_.amount).sum
      users.foreach { user =>
        outputCollector.accept(
          CombinedAccount(key, user.get("age").asInstanceOf[Integer], sum)
        )
      }
    }
    // #SortMergeBucketExample_transform
  }

  def secondaryReadExample(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    // #SortMergeBucketExample_secondary_read
    sc.sortMergeGroupByKey(
      classOf[String], // primary key class
      classOf[String], // secondary key class
      ParquetAvroSortedBucketIO
        .read(new TupleTag[Account]("account"), classOf[Account])
        .from(args("accounts"))
    ).map { case ((primaryKey, secondaryKey), elements) =>
    // ...
    }
    // #SortMergeBucketExample_secondary_read
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
    ()
  }
}
