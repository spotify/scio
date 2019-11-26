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
import com.spotify.scio.avro.{Account, GenericRecordIO, SpecificRecordIO}
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType

import scala.collection.JavaConverters._
import scala.util.Random

object SortMergeBucketExample {
  lazy val UserDataSchema: Schema = Schema.createRecord(
    "UserData", "doc", "com.spotify.scio.examples.extra", false,
    List(
      new Field("userId", Schema.create(Schema.Type.INT), "doc", ""),
      new Field("age", Schema.create(Schema.Type.INT), "doc", -1)
    ).asJava)

  def user(id: Int, age: Int): GenericRecord = {
    val gr = new GenericData.Record(UserDataSchema)
    gr.put("userId", id)
    gr.put("age", age)

    gr
  }

  def account(id: Int, name: String, `type`: String, amount: Double): Account = {
    Account.newBuilder()
      .setId(id)
      .setName(name)
      .setType(`type`)
      .setAmount(amount)
      .build()
  }
}

object SortMergeBucketWriteExample {
  import com.spotify.scio.smb._

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    val outputL = args("outputL")
    val outputR = args("outputR")

    implicit val coder: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

    sc.parallelize(1 to 500)
      .map { i => SortMergeBucketExample.user(i, Random.nextInt(100)) }
      .saveAsSortedBucket(
        classOf[Integer],
        outputL,
        SortMergeBucketExample.UserDataSchema,
        "userId",
        HashType.MURMUR3_32,
        1
      )

    sc.parallelize(250 to 750)
      .map { i => SortMergeBucketExample.account(
        i, s"user$i", s"type${i % 5}", Random.nextDouble() * 1000
      ) }
      .saveAsSortedBucket(
        classOf[Integer],
        outputR,
        "id",
        HashType.MURMUR3_32,
        1
      )

    sc.run().waitUntilDone()
    ()
  }
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
      GenericRecordIO(inputL, SortMergeBucketExample.UserDataSchema),
      SpecificRecordIO[Account](inputR)
    ).map { case (userId, (userData, account)) =>
        s"$userId\t${userData.get("age")}\t${account.getAmount}"
    }.saveAsTextFile(output)

    sc.run().waitUntilDone()
    ()
  }
}
