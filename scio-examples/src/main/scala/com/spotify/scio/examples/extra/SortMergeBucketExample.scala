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
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.Account
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._
import scala.util.Random

object SortMergeBucketExample {
  lazy val UserDataSchema: Schema = Schema.createRecord(
    "UserData",
    "doc",
    "com.spotify.scio.examples.extra",
    false,
    List(
      new Field("userId", Schema.create(Schema.Type.INT), "doc", ""),
      new Field("age", Schema.create(Schema.Type.INT), "doc", -1)
    ).asJava
  )

  def user(id: Int, age: Int): GenericRecord = {
    val gr = new GenericData.Record(UserDataSchema)
    gr.put("userId", id)
    gr.put("age", age)

    gr
  }

  def account(id: Int, name: String, `type`: String, amount: Double): Account =
    Account
      .newBuilder()
      .setId(id)
      .setName(name)
      .setType(`type`)
      .setAmount(amount)
      .build()
}

object SortMergeBucketWriteExample {
  import com.spotify.scio.smb._

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    val outputL = args("outputL")
    val outputR = args("outputR")

    implicit val coder: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

    sc.parallelize(0 until 500)
      .map { i =>
        SortMergeBucketExample.user(i, Random.nextInt(100))
      }
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[Integer], "userId", SortMergeBucketExample.UserDataSchema)
          .to(outputL)
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(2)
          .withNumShards(1)
      )

    sc.parallelize(250 until 750)
      .map { i =>
        SortMergeBucketExample.account(
          i,
          s"user$i",
          s"type${i % 5}",
          Random.nextDouble() * 1000
        )
      }
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write[Integer, Account](classOf[Integer], "id", classOf[Account])
          .to(outputR)
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(1)
          .withNumShards(1)
      )

    sc.run().waitUntilDone()
    ()
  }
}

case class UserAccountData(userId: Int, age: Int, balance: Double) {
  override def toString: String = s"$userId\t$age\t$balance"
}

object SortMergeBucketJoinExample {
  import com.spotify.scio.smb._

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    val inputL = args("inputL")
    val inputR = args("inputR")
    val output = args("output")

    implicit val coder: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

    sc.sortMergeJoin(
        classOf[Integer],
        AvroSortedBucketIO
          .read(new TupleTag[GenericRecord](inputL), SortMergeBucketExample.UserDataSchema)
          .from(inputL),
        AvroSortedBucketIO
          .read(new TupleTag[Account](inputR), classOf[Account])
          .from(inputR)
      )
      .map {
        case (userId, (userData, account)) =>
          UserAccountData(userId, userData.get("age").toString.toInt, account.getAmount)
      }
      .saveAsTextFile(output)

    sc.run().waitUntilDone()
    ()
  }
}
