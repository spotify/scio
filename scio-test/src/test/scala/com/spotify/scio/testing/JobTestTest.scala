/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.testing

import com.google.common.collect.ImmutableMap
import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils.{newGenericRecord, newSpecificRecord}
import com.spotify.scio.avro.{AvroUtils, TestRecord}
import com.spotify.scio.bigquery._
import org.apache.avro.generic.GenericRecord

import scala.io.Source

object ObjectFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.objectFile[Int](args("input"))
      .map(_ * 10)
      .saveAsObjectFile(args("output"))
    sc.close()
  }
}

object SpecificAvroFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.avroFile[TestRecord](args("input"))
      .saveAsAvroFile(args("output"))
    sc.close()
  }
}

object GenericAvroFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.avroFile[GenericRecord](args("input"), AvroUtils.schema)
      .saveAsAvroFile(args("output"))
    sc.close()
  }
}

object BigQueryJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.bigQueryTable(args("input"))
      .saveAsBigQuery(args("output"))
    sc.close()
  }
}

object DatastoreJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.datastore(args("input"), null)
      .saveAsDatastore(args("output"))
    sc.close()
  }
}

object PubsubJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.pubsubTopic(args("input"), null)
      .map(_ + "X")
      .saveAsPubsub(args("output"))
    sc.close()
  }
}

object TableRowJsonJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.tableRowJsonFile(args("input"))
      .saveAsTableRowJsonFile(args("output"))
    sc.close()
  }
}

object TextFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .map(_ + "X")
      .saveAsTextFile(args("output"))
    sc.close()
  }
}

object DistCacheJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dc = sc.distCache(args("distCache"))(f => Source.fromFile(f).getLines().toSeq)
    sc.textFile(args("input"))
      .flatMap(x => dc().map(x + _))
      .saveAsTextFile(args("output"))
    sc.close()
  }
}

// scalastyle:off no.whitespace.before.left.bracket
class JobTestTest extends PipelineSpec {

  def testObjectFileJob(xs: Int*): Unit = {
    JobTest[ObjectFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(ObjectFileIO("in.avro"), Seq(1, 2, 3))
      .output[Int](ObjectFileIO("out.avro"))(_ should containInAnyOrder (xs))
      .run()
  }

  "JobTest" should "pass correct ObjectFileIO" in {
    testObjectFileJob(10, 20, 30)
  }

  it should "fail incorrect ObjectFileIO" in {
    an [AssertionError] should be thrownBy { testObjectFileJob(10, 20) }
    an [AssertionError] should be thrownBy { testObjectFileJob(10, 20, 30, 40) }
  }

  def testSpecificAvroFileJob(xs: Seq[TestRecord]): Unit = {
    JobTest[SpecificAvroFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO("in.avro"), (1 to 3).map(newSpecificRecord))
      .output[TestRecord](AvroIO("out.avro"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct specific AvroFileIO" in {
    testSpecificAvroFileJob((1 to 3).map(newSpecificRecord))
  }

  it should "fail incorrect specific AvroFileIO" in {
    an [AssertionError] should be thrownBy {
      testSpecificAvroFileJob((1 to 2).map(newSpecificRecord))
    }
    an [AssertionError] should be thrownBy {
      testSpecificAvroFileJob((1 to 4).map(newSpecificRecord))
    }
  }

  def testGenericAvroFileJob(xs: Seq[GenericRecord]): Unit = {
    JobTest[GenericAvroFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO("in.avro"), (1 to 3).map(newGenericRecord))
      .output[GenericRecord](AvroIO("out.avro"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct generic AvroFileIO" in {
    testGenericAvroFileJob((1 to 3).map(newGenericRecord))
  }

  it should "fail incorrect generic AvroFileIO" in {
    an [AssertionError] should be thrownBy {
      testGenericAvroFileJob((1 to 2).map(newGenericRecord))
    }
    an [AssertionError] should be thrownBy {
      testGenericAvroFileJob((1 to 4).map(newGenericRecord))
    }
  }

  def newTableRow(i: Int): TableRow = TableRow("int_field" -> i)

  def testBigQuery(xs: Seq[TableRow]): Unit = {
    JobTest[BigQueryJob.type]
      .args("--input=table.in", "--output=table.out")
      .input(BigQueryIO("table.in"), (1 to 3).map(newTableRow))
      .output[TableRow](BigQueryIO("table.out"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct BigQueryJob" in {
    testBigQuery((1 to 3).map(newTableRow))
  }

  it should "fail incorrect BigQueryJob" in {
    an [AssertionError] should be thrownBy { testBigQuery((1 to 2).map(newTableRow)) }
    an [AssertionError] should be thrownBy { testBigQuery((1 to 4).map(newTableRow)) }
  }

  def newEntity(i: Int): Entity = Entity.newBuilder()
    .setKey(makeKey())
    .putAllProperties(ImmutableMap.of("int_field", makeValue(i).build()))
    .build()

  def testDatastore(xs: Seq[Entity]): Unit = {
    JobTest[DatastoreJob.type]
      .args("--input=store.in", "--output=store.out")
      .input(DatastoreIO("store.in"), (1 to 3).map(newEntity))
      .output[Entity](DatastoreIO("store.out"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct DatastoreJob" in {
    testDatastore((1 to 3).map(newEntity))
  }

  it should "fail incorrect DatastoreJob" in {
    an [AssertionError] should be thrownBy { testDatastore((1 to 2).map(newEntity)) }
    an [AssertionError] should be thrownBy { testDatastore((1 to 4).map(newEntity)) }
  }

  def testPubsubJob(xs: String*): Unit = {
    JobTest[PubsubJob.type]
      .args("--input=in", "--output=out")
      .input(PubsubIO("in"), Seq("a", "b", "c"))
      .output[String](PubsubIO("out"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct PubsubIO" in {
    testTextFileJob("aX", "bX", "cX")
  }

  it should "fail incorrect PubsubIO" in {
    an [AssertionError] should be thrownBy { testTextFileJob("aX", "bX") }
    an [AssertionError] should be thrownBy { testTextFileJob("aX", "bX", "cX", "dX") }
  }

  def testTableRowJson(xs: Seq[TableRow]): Unit = {
    JobTest[TableRowJsonJob.type]
      .args("--input=in.json", "--output=out.json")
      .input(TableRowJsonIO("in.json"), (1 to 3).map(newTableRow))
      .output[TableRow](TableRowJsonIO("out.json"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct TableRowJsonIO" in {
    testTableRowJson((1 to 3).map(newTableRow))
  }

  it should "fail incorrect TableRowJsonIO" in {
    an [AssertionError] should be thrownBy { testTableRowJson((1 to 2).map(newTableRow)) }
    an [AssertionError] should be thrownBy { testTableRowJson((1 to 4).map(newTableRow)) }
  }

  def testTextFileJob(xs: String*): Unit = {
    JobTest[TextFileJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b", "c"))
      .output[String](TextIO("out.txt"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct TextFileIO" in {
    testTextFileJob("aX", "bX", "cX")
  }

  it should "fail incorrect TextFileIO" in {
    an [AssertionError] should be thrownBy { testTextFileJob("aX", "bX") }
    an [AssertionError] should be thrownBy { testTextFileJob("aX", "bX", "cX", "dX") }
  }

  def testDistCacheJob(xs: String*): Unit = {
    JobTest[DistCacheJob.type]
      .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
      .output[String](TextIO("out.txt"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct DistCacheIO" in {
    testDistCacheJob("a1", "a2", "b1", "b2")
  }

  it should "fail incorrect DistCacheIO" in {
    an [AssertionError] should be thrownBy { testDistCacheJob("a1", "a2", "b1") }
    an [AssertionError] should be thrownBy { testDistCacheJob("a1", "a2", "b1", "b2", "c3", "d4") }
  }

  it should "fail unmatched test input" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .input(TextIO("unmatched.txt"), Seq("X", "Y"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message "requirement failed: Unmatched test input: TextIO(unmatched.txt)"
  }

  it should "fail unmatched test output" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .output[String](TextIO("unmatched.txt"))(_ should containInAnyOrder (Seq("X", "Y")))
        .run()
    } should have message "requirement failed: Unmatched test output: TextIO(unmatched.txt)"
  }

  it should "fail unmatched test dist cache" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .distCache(DistCacheIO("unmatched.txt"), Seq("X", "Y"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message
      "requirement failed: Unmatched test dist cache: DistCacheIO(unmatched.txt)"
  }

}
// scalastyle:on no.whitespace.before.left.bracket
