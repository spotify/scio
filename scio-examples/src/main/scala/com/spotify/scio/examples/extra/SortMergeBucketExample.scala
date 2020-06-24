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
// --outputL=[OUTPUT]--outputR=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketJoinExample
// --inputL=[INPUT]--inputR=[INPUT] --output=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketTransformExample
// --inputL=[INPUT]--inputR=[INPUT] --output=[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.Account
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema.Field
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{JsonProperties, Schema}
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.TargetParallelism
import org.apache.beam.sdk.values.TupleTag

import scala.jdk.CollectionConverters._
import scala.util.Random

object SortMergeBucketExample {
  lazy val UserDataSchema: Schema = Schema.createRecord(
    "UserData",
    "doc",
    "com.spotify.scio.examples.extra",
    false,
    List(
      new Field("userId", Schema.create(Schema.Type.INT), "doc", JsonProperties.NULL_VALUE),
      new Field("age", Schema.create(Schema.Type.INT), "doc", JsonProperties.NULL_VALUE)
    ).asJava
  )

  def user(id: Int, age: Int): GenericRecord = {
    val gr = new GenericData.Record(UserDataSchema)
    gr.put("userId", id)
    gr.put("age", age)

    gr
  }
}

object SortMergeBucketWriteExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    sc.parallelize(0 until 500)
      .map(i => SortMergeBucketExample.user(i, Random.nextInt(100)))
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[Integer], "userId", SortMergeBucketExample.UserDataSchema)
          .to(args("userOutput"))
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(2)
          .withNumShards(1)
      )

    // #SortMergeBucketExample_sink
    sc.parallelize(250 until 750)
      .map { i =>
        Account
          .newBuilder()
          .setId(i)
          .setName(s"user$i")
          .setType(s"type${i % 5}")
          .setAmount(Random.nextDouble() * 1000)
          .build()
      }
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write[Integer, Account](classOf[Integer], "id", classOf[Account])
          .to(args("accountOutput"))
          .withSorterMemoryMb(128)
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(1)
          .withNumShards(1)
      )
    // #SortMergeBucketExample_sink
    sc.run().waitUntilDone()
    ()
  }
}

object SortMergeBucketJoinExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  case class UserAccountData(userId: Int, age: Int, balance: Double) {
    override def toString: String = s"$userId\t$age\t$balance"
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    val mapFn: ((Integer, (GenericRecord, Account))) => UserAccountData = {
      case (userId, (userData, account)) =>
        UserAccountData(userId, userData.get("age").toString.toInt, account.getAmount)
    }

    // #SortMergeBucketExample_join
    sc.sortMergeJoin(
      classOf[Integer],
      AvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("lhs"), SortMergeBucketExample.UserDataSchema)
        .from(args("lhsInput")),
      AvroSortedBucketIO
        .read(new TupleTag[Account]("rhs"), classOf[Account])
        .from(args("rhsInput")),
      TargetParallelism.max()
    ).map(mapFn) // Apply mapping function
      .saveAsTextFile(args("output"))
    // #SortMergeBucketExample_join

    sc.run().waitUntilDone()
    ()
  }
}

object SortMergeBucketTransformExample {
  import com.spotify.scio.smb._

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    // #SortMergeBucketExample_transform
    sc.sortMergeTransform(
      classOf[Integer],
      AvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("lhs"), SortMergeBucketExample.UserDataSchema)
        .from(args("lhsInput")),
      AvroSortedBucketIO
        .read(new TupleTag[Account]("rhs"), classOf[Account])
        .from(args("rhsInput"))
    ).to(
      AvroSortedBucketIO
        .transformOutput(classOf[Integer], "userId", classOf[Account])
        .to(args("output"))
    ).via {
      case (key, (users, accounts), outputCollector) =>
        users.foreach { user =>
          outputCollector.accept(
            Account
              .newBuilder()
              .setId(key)
              .setName(user.get("userId").toString)
              .setType("combinedAmount")
              .setAmount(accounts.foldLeft(0.0)(_ + _.getAmount))
              .build()
          )
        }
    }
    // #SortMergeBucketExample_transform

    sc.run().waitUntilDone()
    ()
  }
}
