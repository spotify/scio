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
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.smb._
import com.spotify.scio.testing._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser
import org.apache.beam.sdk.values.TupleTag

import scala.collection.compat._

object ParquetEndToEndTest {
  val eventSchema: Schema = SchemaBuilder
    .record("Event")
    .namespace("org.apache.beam.sdk.extensions.smb.avro")
    .fields()
    .requiredString("user")
    .requiredString("event")
    .requiredString("country")
    .requiredInt("timestamp")
    .endRecord()

  val userSchema: Schema = SchemaBuilder
    .record("User")
    .namespace("org.apache.beam.sdk.extensions.smb.avro")
    .fields()
    .requiredString("name")
    .requiredString("country")
    .requiredInt("age")
    .endRecord()

  def avroEvent(x: Int): GenericRecord = new GenericRecordBuilder(eventSchema)
    .set("user", s"user${x % 10 + 1}")
    .set("event", s"event$x")
    .set("country", s"country${x % 2 + 1}")
    .set("timestamp", x)
    .build()

  def avroUser(x: Int): GenericRecord = new GenericRecordBuilder(userSchema)
    .set("name", s"user$x")
    .set("country", s"country${x % 2 + 1}")
    .set("age", x)
    .build()

  case class Event(user: String, event: String, country: String, timestamp: Int)
  case class User(name: String, country: String, age: Int)

  object Event {
    def apply(x: Int): Event = Event(s"user${x % 10 + 1}", s"event$x", s"country${x % 2 + 1}", x)
  }

  object User {
    def apply(x: Int): User = User(s"user$x", s"country${x % 2 + 1}", x)
  }

  val avroEvents: Seq[GenericRecord] = (1 to 100).map(avroEvent)
  val avroUsers: Seq[GenericRecord] = (1 to 15).map(avroUser)

  val avroEventCoder: Coder[GenericRecord] = avroGenericRecordCoder(eventSchema)
  val avroUserCoder: Coder[GenericRecord] = avroGenericRecordCoder(userSchema)

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
      .parallelize(avroEvents)(avroEventCoder)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "user", eventSchema)
          .to(eventsDir.toString)
          .withNumBuckets(1)
      )
    sc1
      .parallelize(avroUsers)(avroUserCoder)
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
      )(Coder[String], avroEventCoder, avroUserCoder)

    val userMap = avroUsers.groupBy(_.get("name").toString).view.mapValues(_.head).toMap
    val expected = avroEvents.groupBy(_.get("user").toString).toSeq.flatMap { case (k, es) =>
      es.map(e => (k, (e, userMap(k))))
    }
    implicit val c: Coder[(String, (GenericRecord, GenericRecord))] = actual.coder
    actual should containInAnyOrder(expected)
    () // Suppress unused warnings - test is verifying containInAnyOrder assertion
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

    implicit val keyCoder: Coder[CharSequence] =
      Coder.xmap(Coder.stringCoder)(identity, _.toString)

    val sc2 = ScioContext()
    val actual = sc2
      .sortMergeGroupByKey(
        classOf[CharSequence],
        ParquetAvroSortedBucketIO
          .read(new TupleTag[AvroGeneratedUser]("user"), classOf[AvroGeneratedUser])
          .from(usersDir.toString)
      )
      .map(kv => (kv._1.toString, kv._2.toList))
    val expected = users.map(u => (u.getName.toString, List(u)))
    actual should containInAnyOrder(expected)
  }

  it should "support cogrouping typed Parquet" in {
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
      .sortMergeCoGroup(
        classOf[String],
        ParquetTypeSortedBucketIO
          .read[Event](new TupleTag[Event]("user"))
          .from(eventsDir.toString)
          .withConfiguration(ParquetConfiguration.of("foo" -> "Bar")),
        ParquetTypeSortedBucketIO
          .read[User](new TupleTag[User]("name"))
          .from(usersDir.toString)
      )

    val userMap = users.groupBy(_.name).view.toMap
    val eventMap = events.groupBy(_.user).view.toMap
    val expected = userMap.keys.toSeq.map { userName =>
      val eventsForUser = eventMap.getOrElse(userName, Seq.empty)
      (userName, (eventsForUser.toSet, userMap(userName).toSet))
    }
    // Convert actual results to sets for order-independent comparison
    val actualSets = actual.map { case (k, (eventsIter, usersIter)) =>
      (k, (eventsIter.toSet, usersIter.toSet))
    }
    actualSets should containInAnyOrder(expected)

    sc2.run()

    eventsDir.delete()
    usersDir.delete()
  }

  it should "support cogrouping typed Parquet with secondary key" in {
    val eventsDir = tmpDir
    val usersDir = tmpDir
    val sc1 = ScioContext()
    sc1
      .parallelize(events)
      .saveAsSortedBucket(
        ParquetTypeSortedBucketIO
          .write[String, String, Event]("user", "country")
          .to(eventsDir.toString)
          .withNumBuckets(1)
      )
    sc1
      .parallelize(users)
      .saveAsSortedBucket(
        ParquetTypeSortedBucketIO
          .write[String, String, User]("name", "country")
          .to(usersDir.toString)
          .withNumBuckets(1)
      )
    sc1.run()

    val sc2 = ScioContext()
    val actual = sc2
      .sortMergeCoGroup(
        classOf[String],
        classOf[String],
        ParquetTypeSortedBucketIO
          .read[Event](new TupleTag[Event]())
          .from(eventsDir.toString)
          .withConfiguration(ParquetConfiguration.of("foo" -> "Bar")),
        ParquetTypeSortedBucketIO
          .read[User](new TupleTag[User]())
          .from(usersDir.toString)
      )

    val userMap = users.groupBy(u => (u.name, u.country)).view.toMap
    val eventMap = events.groupBy(e => (e.user, e.country)).view.toMap
    // Include all keys from both users and events
    val allKeys = (userMap.keys ++ eventMap.keys).toSet
    val expected = allKeys.toSeq.map { case (userName, country) =>
      val eventsForKey = eventMap.getOrElse((userName, country), Seq.empty)
      val usersForKey = userMap.getOrElse((userName, country), Seq.empty)
      ((userName, country), (eventsForKey.toSet, usersForKey.toSet))
    }
    // Convert actual results to sets for order-independent comparison
    val actualSets = actual.map { case (k, (eventsIter, usersIter)) =>
      (k, (eventsIter.toSet, usersIter.toSet))
    }
    actualSets should containInAnyOrder(expected)

    sc2.run()

    eventsDir.delete()
    usersDir.delete()
  }
}
