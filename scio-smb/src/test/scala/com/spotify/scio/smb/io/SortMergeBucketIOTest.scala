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

package com.spotify.scio.smb.io

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.{GenericRecordIO, SpecificRecordIO}
import com.spotify.scio.smb._
import com.spotify.scio.smb.io.SortMergeBucketIOTestJob.toGenericRecord
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.{AvroBucketMetadata, BucketMetadata}
import org.apache.beam.sdk.io.AvroGeneratedUser
import org.scalatest.Matchers

import scala.collection.JavaConverters._

object SortMergeBucketIOTestJob {
  val schemaOut: Schema = Schema.createRecord(List(
    new Field("key", Schema.create(Schema.Type.STRING), "", "unknown"),
    new Field("color", Schema.create(Schema.Type.STRING), "", "unknown"),
    new Field("number", Schema.create(Schema.Type.INT), "", -1)
  ).asJava)

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdLineArgs)

    sc
      .sortMergeJoin(
        classOf[String],
        SpecificRecordIO[AvroGeneratedUser]("lhsPath"),
        GenericRecordIO("rhsPath", AvroGeneratedUser.SCHEMA$)
      )
      .map { case (k, (l, r)) =>
        toGenericRecord(k, l.getFavoriteColor, r.get("favorite_number").asInstanceOf[Int])
      }.saveAsSortedBucket[String]("output", schemaOut, "key", HashType.MURMUR3_32, 1)

    sc.run().waitUntilDone()
  }

  def toGenericRecord(key: String, color: String, number: Int): GenericRecord = {
    val gr = new GenericData.Record(schemaOut)
    gr.put("key", key)
    gr.put("color", color)
    gr.put("number", number)

    gr
  }
}

class SmbIOTest extends PipelineSpec with Matchers {
  "SortMergeBucketIO" should "work with JobTest" in {
    JobTest[SortMergeBucketIOTestJob.type]
      .inputSmb(SpecificRecordIO[AvroGeneratedUser]("lhsPath"), metadataL, lhs)
      .inputSmb(GenericRecordIO("rhsPath", AvroGeneratedUser.SCHEMA$), metadataR, rhs)
      .output(
        GenericRecordIO("output", SortMergeBucketIOTestJob.schemaOut)
      )(_ should containInAnyOrder(expectedOutput))
      .run()
  }

  private val userA = AvroGeneratedUser.newBuilder()
    .setName("a")
    .setFavoriteColor("red")
    .setFavoriteNumber(1)
    .build()

  private val userB = AvroGeneratedUser.newBuilder()
    .setName("b")
    .setFavoriteColor("blue")
    .setFavoriteNumber(2)
    .build()

  private val userC = AvroGeneratedUser.newBuilder()
    .setName("c")
    .setFavoriteColor("green")
    .setFavoriteNumber(3)
    .build()

  private val userD = AvroGeneratedUser.newBuilder()
    .setName("d")
    .setFavoriteColor("cyan")
    .setFavoriteNumber(4)
    .build()

  private val metadataL: BucketMetadata[String, AvroGeneratedUser] = new AvroBucketMetadata(
    1, 1, classOf[String], HashType.MURMUR3_32, "name")

  private val metadataR: BucketMetadata[String, GenericRecord] = new AvroBucketMetadata(
    1, 1, classOf[String], HashType.MURMUR3_32, "name")

  private val lhs = Seq(userA, userB, userC)
  private val rhs = Seq(userB, userC, userD)

  private val expectedOutput = Seq(
    toGenericRecord(userB.getName, userB.getFavoriteColor, userB.getFavoriteNumber),
    toGenericRecord(userC.getName, userC.getFavoriteColor, userC.getFavoriteNumber)
  )
}
