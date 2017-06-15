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
import com.spotify.scio.util.MockedPrintStream
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.{io => gio}

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
    sc.datastore(args("input"), null, null)
      .saveAsDatastore(args("output"))
    sc.close()
  }
}

object PubsubJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.pubsubTopic[String](args("input"), null)
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

object MaterializeJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val data = sc.textFile(args("input"))
    data.materialize
    data.saveAsTextFile(args("output"))
    sc.close()
  }
}

object CustomIOJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val inputTransform = gio.TextIO.read()
      .from(args("input"))
    val outputTransform = gio.TextIO.write()
      .to(args("output"))
    sc.customInput("TextIn", inputTransform)
      .map(_.toInt)
      .map(_ * 10)
      .map(_.toString)
      .saveAsCustomOutput("TextOut", outputTransform)
    sc.close()
  }
}

object JobWithoutClose {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(1 to 10)
      .count
      .saveAsObjectFile(args("output"))
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
      .input(PubsubIO[String]("in"), Seq("a", "b", "c"))
      .output[String](PubsubIO("out"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct PubsubIO" in {
    testPubsubJob("aX", "bX", "cX")
  }

  it should "fail incorrect PubsubIO" in {
    an [AssertionError] should be thrownBy { testPubsubJob("aX", "bX") }
    an [AssertionError] should be thrownBy { testPubsubJob("aX", "bX", "cX", "dX") }
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

  def testCustomIOJob(xs: String*): Unit = {
    JobTest[CustomIOJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(CustomIO("TextIn"), Seq(1, 2, 3).map(_.toString))
      .output[String](CustomIO("TextOut"))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct CustomIO" in {
    testCustomIOJob("10", "20", "30")
  }

  it should "fail incorrect CustomIO" in {
    an [AssertionError] should be thrownBy { testCustomIOJob("10", "20") }
    an [AssertionError] should be thrownBy { testCustomIOJob("10", "20", "30", "40") }
  }

  // =======================================================================
  // Handling incorrect test wiring
  // =======================================================================

  it should "fail missing test input" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message "requirement failed: Missing test input: TextIO(in.txt), available: []"
  }

  it should "fail misspelled test input" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("bad-in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message
      "requirement failed: Missing test input: TextIO(in.txt), available: [TextIO(bad-in.txt)]"
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

  it should "fail duplicate test input" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .input(TextIO("in.txt"), Seq("X", "Y"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message "requirement failed: Duplicate test input: TextIO(in.txt)"
  }

  it should "fail missing test output" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .run()
    } should have message "requirement failed: Missing test output: TextIO(out.txt), available: []"
  }

  it should "fail misspelled test output" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .output[String](TextIO("bad-out.txt"))(
          _ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .run()
    } should have message
      "requirement failed: Missing test output: TextIO(out.txt), available: [TextIO(bad-out.txt)]"
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

  it should "fail duplicate test output" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("X", "Y")))
        .run()
    } should have message "requirement failed: Duplicate test output: TextIO(out.txt)"
  }

  it should "fail missing test dist cache" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message
      "requirement failed: Missing test dist cache: DistCacheIO(dc.txt), available: []"
  }

  it should "fail misspelled test dist cache" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("bad-dc.txt"), Seq("1", "2"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message
      "requirement failed: Missing test dist cache: DistCacheIO(dc.txt), available: " +
        "[DistCacheIO(bad-dc.txt)]"
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

  it should "fail duplicate test dist cache" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .distCache(DistCacheIO("dc.txt"), Seq("X", "Y"))
        .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a1", "a2", "b1", "b2")))
        .run()
    } should have message
      "requirement failed: Duplicate test dist cache: DistCacheIO(dc.txt)"
  }

  it should "ignore materialize" in {
    JobTest[MaterializeJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .output[String](TextIO("out.txt"))(_ should containInAnyOrder (Seq("a", "b")))
      .run()
  }

  it should "fail job without close" in {
    the [IllegalArgumentException] thrownBy {
      JobTest[JobWithoutClose.type]
        .args("--output=out.avro")
        .output[Long](ObjectFileIO("out.avro"))(_ should containInAnyOrder (Seq(10L)))
        .run()
    } should have message
      "requirement failed: ScioContext was not closed. Did you forget close()?"
  }

  // =======================================================================
  // Tests of JobTest testing wiring
  // =======================================================================

  class JobTestFromType extends PipelineSpec {
    "JobTestFromType" should "work" in {
      JobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO("in.avro"), Seq(1, 2, 3))
        .output[Int](ObjectFileIO("out.avro"))(_ should containInAnyOrder(Seq(1, 2, 3)))
    }
  }

  class JobTestFromString extends PipelineSpec {
    "JobTestFromString" should "work" in {
      JobTest("com.spotify.scio.testing.ObjectFileJob")
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO("in.avro"), Seq(1, 2, 3))
        .output[Int](ObjectFileIO("out.avro"))(_ should containInAnyOrder (Seq(1, 2, 3)))
    }
  }

  class MultiJobTest extends PipelineSpec {
    "MultiJobTest" should "work" in {
      JobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO("in.avro"), Seq(1, 2, 3))
        .output[Int](ObjectFileIO("out.avro"))(_ should containInAnyOrder (Seq(1, 2, 3)))

      testBigQuery((1 to 3).map(newTableRow))

      JobTest[ObjectFileJob.type]
        .args("--input=in2.avro", "--output=out2.avro")
        .input(ObjectFileIO("in2.avro"), Seq(1, 2, 3))
        .output[Int](ObjectFileIO("out2.avro"))(_ should containInAnyOrder (Seq(1, 2, 3)))
    }
  }

  class OriginalJobTest extends PipelineSpec {
    import com.spotify.scio.testing.{JobTest => InternalJobTest}
    "OriginalJobTest" should "work" in {
      InternalJobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO("in.avro"), Seq(1, 2, 3))
        .output[Int](ObjectFileIO("out.avro"))(_ should containInAnyOrder (Seq(1, 2, 3)))
    }
  }

  // scalastyle:off line.contains.tab
  // scalastyle:off line.size.limit
  private val runMissedMessage = """|- should work \*\*\* FAILED \*\*\*
                                    |  Did you forget run\(\)\?
                                    |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                                    |  	args: --input=in.avro --output=out.avro
                                    |  	distCache: Map\(\)
                                    |  	inputs: ObjectFileIO\(in.avro\) -> List\(1, 2, 3\) \(JobTestTest.scala:.*\)""".stripMargin
  // scalastyle:on line.size.limit
  // scalastyle:on line.contains.tab

  it should "enforce run() on JobTest from class type" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new JobTestFromType().execute("JobTestFromType should work", color = false)
    }
    stdOutMock.message.mkString("") should include regex runMissedMessage
  }

  it should "enforce run() on multi JobTest" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new MultiJobTest().execute("MultiJobTest should work", color = false)
    }
    // scalastyle:off line.contains.tab
    // scalastyle:off line.size.limit
    val msg = """|- should work \*\*\* FAILED \*\*\*
                 |  Did you forget run\(\)\?
                 |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                 |  	args: --input=in.avro --output=out.avro
                 |  	distCache: Map\(\)
                 |  	inputs: ObjectFileIO\(in.avro\) -> List\(1, 2, 3\)
                 |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                 |  	args: --input=in2.avro --output=out2.avro
                 |  	distCache: Map\(\)
                 |  	inputs: ObjectFileIO\(in2.avro\) -> List\(1, 2, 3\) \(JobTestTest.scala:.*\)""".stripMargin
    // scalastyle:on line.size.limit
    // scalastyle:on line.contains.tab
    stdOutMock.message.mkString("") should include regex msg
  }

  it should "enforce run() on JobTest from string class" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new JobTestFromString().execute("JobTestFromString should work", color = false)
    }
    stdOutMock.message.mkString("") should include regex runMissedMessage
  }

  it should "not enforce run() on internal JobTest" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new OriginalJobTest().execute("OriginalJobTest should work", color = false)
    }
    stdOutMock.message.mkString("") shouldNot include regex runMissedMessage
  }

  // =======================================================================
  // Test invalid TestIO
  // =======================================================================

  "TestIO" should "not allow null keys" in {
    def test[T](testIO: => TestIO[T], repr: String): Unit = {
      the [IllegalArgumentException] thrownBy {
        testIO
      } should have message s"requirement failed: $repr has null key"
    }
    test(ObjectFileIO(null), "ObjectFileIO(null)")
    test(AvroIO(null), "AvroIO(null)")
    test(BigQueryIO(null), "BigQueryIO(null)")
    test(DatastoreIO(null), "DatastoreIO(null,null,null)")
    test(ProtobufIO(null), "ProtobufIO(null)")
    test(PubsubIO(null), "PubsubIO(null)")
    test(TableRowJsonIO(null), "TableRowJsonIO(null)")
    test(TextIO(null), "TextIO(null)")
    test(TFRecordIO(null), "TFRecordIO(null)")
  }

  it should "not allow empty keys" in {
    def test[T](testIO: => TestIO[T], repr: String): Unit = {
      the [IllegalArgumentException] thrownBy {
        testIO
      } should have message s"requirement failed: $repr has empty string key"
    }
    test(ObjectFileIO(""), "ObjectFileIO()")
    test(AvroIO(""), "AvroIO()")
    test(BigQueryIO(""), "BigQueryIO()")
    test(DatastoreIO(""), "DatastoreIO(,null,null)")
    test(ProtobufIO(""), "ProtobufIO()")
    test(PubsubIO(""), "PubsubIO()")
    test(TableRowJsonIO(""), "TableRowJsonIO()")
    test(TextIO(""), "TextIO()")
    test(TFRecordIO(""), "TFRecordIO()")
  }

  "runWithContext" should "fail input with message" in {
    val msg = "requirement failed: Missing test data. Are you reading input outside of JobTest?"
    the [IllegalArgumentException] thrownBy {
      runWithContext(_.textFile("in.txt"))
    } should have message msg
  }

  it should "fail output with message" in {
    val msg = "requirement failed: Missing test data. Are you writing output outside of JobTest?"
    the [IllegalArgumentException] thrownBy {
      runWithContext(_.parallelize(1 to 10).materialize)
    } should have message msg
  }

  it should "fail dist cache with message" in {
    val msg = "requirement failed: Missing test data. Are you using dist cache outside of JobTest?"
    the [IllegalArgumentException] thrownBy {
      runWithContext(_.distCache("in.txt")(f => Source.fromFile(f).getLines().toSeq))
    } should have message msg
  }

}
// scalastyle:on no.whitespace.before.left.bracket
