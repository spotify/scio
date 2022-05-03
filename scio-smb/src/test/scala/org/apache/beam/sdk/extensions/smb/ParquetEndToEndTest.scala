/*
 * Copyright 2021 Spotify AB.
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

package org.apache.beam.sdk.extensions.smb

import java.io.File
import java.util.UUID
import com.spotify.scio._
import com.spotify.scio.smb._
import com.spotify.scio.testing._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.io.AvroGeneratedUser
import org.apache.beam.sdk.values.TupleTag

import scala.jdk.CollectionConverters._
import scala.collection.compat._ // scalafix:ok

object ParquetEndToEndTest {
  val eventSchema: Schema = Schema.createRecord(
    "Event",
    "",
    "org.apache.beam.sdk.extensions.smb.avro",
    false,
    List(
      new Schema.Field("user", Schema.create(Schema.Type.STRING), "", ""),
      new Schema.Field("event", Schema.create(Schema.Type.STRING), "", ""),
      new Schema.Field("timestamp", Schema.create(Schema.Type.INT), "", 0)
    ).asJava
  )

  val userSchema: Schema = Schema.createRecord(
    "User",
    "",
    "org.apache.beam.sdk.extensions.smb.avro",
    false,
    List(
      new Schema.Field("name", Schema.create(Schema.Type.STRING), "", ""),
      new Schema.Field("age", Schema.create(Schema.Type.INT), "", 0)
    ).asJava
  )

  def avroEvent(x: Int): GenericRecord = new GenericRecordBuilder(eventSchema)
    .set("user", s"user${x % 10 + 1}")
    .set("event", s"event$x")
    .set("timestamp", x)
    .build()

  def avroUser(x: Int): GenericRecord = new GenericRecordBuilder(userSchema)
    .set("name", s"user$x")
    .set("age", x)
    .build()

  case class Event(user: String, event: String, timestamp: Int)
  case class User(name: String, age: Int)

  object Event {
    def apply(x: Int): Event = Event(s"user${x % 10 + 1}", s"event$x", x)
  }

  object User {
    def apply(x: Int): User = User(s"user$x", x)
  }

  val avroEvents: Seq[GenericRecord] = (1 to 100).map(avroEvent)
  val avroUsers: Seq[GenericRecord] = (1 to 15).map(avroUser)
  val events: Seq[Event] = (1 to 100).map(Event(_))
  val users: Seq[User] = (1 to 15).map(User(_))
}

class ParquetEndToEndTest extends PipelineSpec {
  import ParquetEndToEndTest._

  def tmpDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())

  "Parquet SMB" should "support writing Avro reading typed" in {
    val eventsDir = tmpDir
    val usersDir = tmpDir

    val sc1 = ScioContext()
    // Write one with keyClass = CharSequence and one with String
    // Downstream should be able to handle the difference
    sc1
      .parallelize(avroEvents)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "user", eventSchema)
          .to(eventsDir.toString)
          .withNumBuckets(1)
      )
    sc1
      .parallelize(avroUsers)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[String], "name", userSchema)
          .to(usersDir.toString)
          .withNumBuckets(1)
      )
    sc1.run()

    val sc2 = ScioContext()
    val actual = sc2
      .sortMergeJoin(
        classOf[String],
        ParquetTypeSortedBucketIO
          .read[Event](new TupleTag[Event]("lhs"))
          .from(eventsDir.toString),
        ParquetTypeSortedBucketIO
          .read[User](new TupleTag[User]("rhs"))
          .from(usersDir.toString)
      )
    val userMap = users.groupBy(_.name).view.mapValues(_.head).toMap
    val expected = events.groupBy(_.user).toSeq.flatMap { case (k, es) =>
      es.map(e => (k, (e, userMap(k))))
    }
    actual should containInAnyOrder(expected)
    sc2.run()

    eventsDir.delete()
    usersDir.delete()
  }

  it should "support writing typed reading Avro" in {
    val eventsDir = tmpDir
    val usersDir = tmpDir

    val sc1 = ScioContext()
    sc1
      .parallelize(events)
      .saveAsSortedBucket(
        ParquetTypeSortedBucketIO
          .write[String, Event]("user")
          .to(eventsDir.toString)
          .withNumBuckets(1)
      )
    sc1
      .parallelize(users)
      .saveAsSortedBucket(
        ParquetTypeSortedBucketIO
          .write[String, User]("name")
          .to(usersDir.toString)
          .withNumBuckets(1)
      )
    sc1.run()

    val sc2 = ScioContext()
    val actual = sc2
      .sortMergeJoin(
        classOf[String],
        ParquetAvroSortedBucketIO
          .read(new TupleTag[GenericRecord]("lhs"), eventSchema)
          .from(eventsDir.toString),
        ParquetAvroSortedBucketIO
          .read(new TupleTag[GenericRecord]("rhs"), userSchema)
          .from(usersDir.toString)
      )
    val userMap = avroUsers.groupBy(_.get("name").toString).mapValues(_.head)
    val expected = avroEvents.groupBy(_.get("user").toString).toSeq.flatMap { case (k, es) =>
      es.map(e => (k, (e, userMap(k))))
    }
    actual should containInAnyOrder(expected)
    sc2.run()

    eventsDir.delete()
    usersDir.delete()
  }

  it should "support specific records" in {
    val usersDir = tmpDir

    val sc1 = ScioContext()

    val users = (1 to 100).map { i =>
      AvroGeneratedUser
        .newBuilder()
        .setName(s"user$i")
        .setFavoriteColor(s"color$i")
        .setFavoriteNumber(i)
        .build()
    }
    sc1
      .parallelize(users)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "name", classOf[AvroGeneratedUser])
          .to(usersDir.toString)
          .withNumBuckets(1)
      )
    sc1.run()

    val sc2 = ScioContext()
    val actual = sc2
      .sortMergeGroupByKey(
        classOf[CharSequence],
        ParquetAvroSortedBucketIO
          .read(new TupleTag[AvroGeneratedUser]("user"), classOf[AvroGeneratedUser])
          .from(usersDir.toString)
      )
      .map(kv => (kv._1.toString, kv._2.toList))
    val expected = users.map(u => (u.getName, List(u)))
    actual should containInAnyOrder(expected)
  }
}
