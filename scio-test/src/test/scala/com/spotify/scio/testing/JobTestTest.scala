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

package com.spotify.scio.testing

import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils.{newGenericRecord, newSpecificRecord}
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.util.MockedPrintStream
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.metrics.DistributionResult
import org.apache.beam.sdk.{io => beam}
import org.scalatest.exceptions.TestFailedException

import scala.io.Source
import org.apache.beam.sdk.metrics.{Counter, Distribution, Gauge}

object ObjectFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.objectFile[Int](args("input"))
      .map(_ * 10)
      .saveAsObjectFile(args("output"))
    sc.run()
    ()
  }
}

object SpecificAvroFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.avroFile[TestRecord](args("input"))
      .saveAsAvroFile(args("output"))
    sc.run()
    ()
  }
}

object GenericAvroFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit val coder = Coder.avroGenericRecordCoder(AvroUtils.schema)
    sc.avroFile(args("input"), AvroUtils.schema)
      .saveAsAvroFile(args("output"), schema = AvroUtils.schema)
    sc.run()
    ()
  }
}

object GenericParseFnAvroFileJob {

  implicit val coder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(AvroUtils.schema)

  // A class with some fields from the Avro Record
  case class PartialFieldsAvro(intField: Int)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parseAvroFile[PartialFieldsAvro](args("input"))((gr: GenericRecord) =>
      PartialFieldsAvro(gr.get("int_field").asInstanceOf[Int])
    ).map(a => AvroUtils.newGenericRecord(a.intField))
      .saveAsAvroFile(args("output"), schema = AvroUtils.schema)
    sc.run()
    ()
  }
}

object TextFileJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .map(_ + "X")
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object DistCacheJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dc =
      sc.distCache(args("distCache"))(f => Source.fromFile(f).getLines().toSeq)
    sc.textFile(args("input"))
      .flatMap(x => dc().map(x + _))
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object MaterializeJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val data = sc.textFile(args("input"))
    data.materialize
    data.saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object CustomIOJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val inputTransform = beam.TextIO
      .read()
      .from(args("input"))
    val outputTransform = beam.TextIO
      .write()
      .to(args("output"))
    sc.customInput("TextIn", inputTransform)
      .map(_.toInt)
      .map(_ * 10)
      .map(_.toString)
      .saveAsCustomOutput("TextOut", outputTransform)
    sc.run()
    ()
  }
}

object ReadAllJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.textFile(args("input"))
      .readFiles
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object ReadAllBytesJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .readFilesAsBytes
      .map(new String(_))
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object JobWithoutClose {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(1 to 10)
      .count
      .saveAsObjectFile(args("output"))
    ()
  }
}

object JobWithDuplicateInput {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
    sc.textFile(args("input"))
    sc.run()
    ()
  }
}

object JobWithDuplicateOutput {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(1 to 10)
      .saveAsTextFile(args("output"))

    sc.parallelize(1 to 5)
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}

object MetricsJob {
  val counter: Counter = ScioMetrics.counter("counter")
  val distribution: Distribution = ScioMetrics.distribution("distribution")
  val gauge: Gauge = ScioMetrics.gauge("gauge")

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(1 to 10)
      .map { x =>
        counter.inc()
        distribution.update(x)
        gauge.set(x)
        x
      }
    sc.run()
    ()
  }
}

class JobTestTest extends PipelineSpec {
  def testObjectFileJob(xs: Int*): Unit =
    JobTest[ObjectFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
      .output(ObjectFileIO[Int]("out.avro"))(coll => coll should containInAnyOrder(xs))
      .run()

  "JobTest" should "pass correct ObjectFileIO" in {
    testObjectFileJob(10, 20, 30)
  }

  it should "fail incorrect ObjectFileIO" in {
    an[AssertionError] should be thrownBy { testObjectFileJob(10, 20) }
    an[AssertionError] should be thrownBy { testObjectFileJob(10, 20, 30, 40) }
  }

  def testSpecificAvroFileJob(xs: Seq[TestRecord]): Unit =
    JobTest[SpecificAvroFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO[TestRecord]("in.avro"), (1 to 3).map(newSpecificRecord))
      .output(AvroIO[TestRecord]("out.avro"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct specific AvroFileIO" in {
    testSpecificAvroFileJob((1 to 3).map(newSpecificRecord))
  }

  it should "fail incorrect specific AvroFileIO" in {
    an[AssertionError] should be thrownBy {
      testSpecificAvroFileJob((1 to 2).map(newSpecificRecord))
    }
    an[AssertionError] should be thrownBy {
      testSpecificAvroFileJob((1 to 4).map(newSpecificRecord))
    }
  }

  def testGenericAvroFileJob(xs: Seq[GenericRecord]): Unit = {
    implicit val coder = Coder.avroGenericRecordCoder
    JobTest[GenericAvroFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO[GenericRecord]("in.avro"), (1 to 3).map(newGenericRecord))
      .output(AvroIO[GenericRecord]("out.avro"))(coll => coll should containInAnyOrder(xs))
      .run()
  }

  it should "pass correct generic AvroFileIO" in {
    testGenericAvroFileJob((1 to 3).map(newGenericRecord))
  }

  it should "fail incorrect generic AvroFileIO" in {
    an[AssertionError] should be thrownBy {
      testGenericAvroFileJob((1 to 2).map(newGenericRecord))
    }
    an[AssertionError] should be thrownBy {
      testGenericAvroFileJob((1 to 4).map(newGenericRecord))
    }
  }

  def testGenericParseAvroFileJob(xs: Seq[GenericRecord]): Unit = {
    import GenericParseFnAvroFileJob.PartialFieldsAvro
    implicit val coder: Coder[GenericRecord] = Coder.avroGenericRecordCoder
    JobTest[GenericParseFnAvroFileJob.type]
      .args("--input=in.avro", "--output=out.avro")
      .input(AvroIO[PartialFieldsAvro]("in.avro"), (1 to 3).map(PartialFieldsAvro))
      .output(AvroIO[GenericRecord]("out.avro")) { coll =>
        coll should containInAnyOrder(xs)
        ()
      }
      .run()
  }

  it should "pass correct generic parseFn AvroFileIO" in {
    testGenericParseAvroFileJob((1 to 3).map(newGenericRecord))
  }

  it should "fail incorrect generic parseFn AvroFileIO" in {
    an[AssertionError] should be thrownBy {
      testGenericParseAvroFileJob((1 to 2).map(newGenericRecord))
    }
    an[AssertionError] should be thrownBy {
      testGenericParseAvroFileJob((1 to 4).map(newGenericRecord))
    }
  }

  def testTextFileJob(xs: String*): Unit =
    JobTest[TextFileJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b", "c"))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct TextIO" in {
    testTextFileJob("aX", "bX", "cX")
  }

  it should "fail incorrect TextIO" in {
    an[AssertionError] should be thrownBy { testTextFileJob("aX", "bX") }
    an[AssertionError] should be thrownBy {
      testTextFileJob("aX", "bX", "cX", "dX")
    }
  }

  def testDistCacheJob(xs: String*): Unit =
    JobTest[DistCacheJob.type]
      .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct DistCacheIO" in {
    testDistCacheJob("a1", "a2", "b1", "b2")
  }

  it should "fail incorrect DistCacheIO" in {
    an[AssertionError] should be thrownBy { testDistCacheJob("a1", "a2", "b1") }
    an[AssertionError] should be thrownBy {
      testDistCacheJob("a1", "a2", "b1", "b2", "c3", "d4")
    }
  }

  def testCustomIOJob(xs: String*): Unit =
    JobTest[CustomIOJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(CustomIO[String]("TextIn"), Seq(1, 2, 3).map(_.toString))
      .output(CustomIO[String]("TextOut"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct CustomIO" in {
    testCustomIOJob("10", "20", "30")
  }

  it should "fail incorrect CustomIO" in {
    an[AssertionError] should be thrownBy { testCustomIOJob("10", "20") }
    an[AssertionError] should be thrownBy {
      testCustomIOJob("10", "20", "30", "40")
    }
  }

  def testReadAllJob(xs: String*): Unit =
    JobTest[ReadAllJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .input(ReadIO("a"), Seq("a1", "a2"))
      .input(ReadIO("b"), Seq("b1", "b2"))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct string ReadIO" in {
    testReadAllJob("a1", "a2", "b1", "b2")
  }

  it should "fail correct string ReadIO" in {
    an[AssertionError] should be thrownBy { testReadAllJob("a1", "a2") }
    an[AssertionError] should be thrownBy {
      testReadAllJob("a1", "a2", "b1", "b2", "c1")
    }
  }

  it should "fail string ReadIO used with TestStream input" in {
    val testStream = testStreamOf[String]
      .addElements("a1", "a2")
      .advanceWatermarkToInfinity()

    the[PipelineExecutionException] thrownBy {
      JobTest[ReadAllJob.type]
        .args("--input=in.txt", "--output=out.txt")
        .input(TextIO("in.txt"), Seq("a"))
        .inputStream(ReadIO("a"), testStream)
        .output(TextIO("out.txt")) { _ => }
        .run()
    } should have message
      s"java.lang.UnsupportedOperationException: Test input TestStream(${testStream.getEvents}) " +
      s"can't be converted to Iterable[T] to test this ScioIO type"
  }

  def testReadAllBytesJob(xs: String*): Unit =
    JobTest[ReadAllBytesJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .input(ReadIO("a"), Seq("a1", "a2").map(_.getBytes))
      .input(ReadIO("b"), Seq("b1", "b2").map(_.getBytes))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct bytes ReadIO" in {
    testReadAllBytesJob("a1", "a2", "b1", "b2")
  }

  it should "fail correct bytes ReadIO" in {
    an[AssertionError] should be thrownBy { testReadAllBytesJob("a1", "a2") }
    an[AssertionError] should be thrownBy {
      testReadAllBytesJob("a1", "a2", "b1", "b2", "c1")
    }
  }

  it should "fail bytes ReadIO used with TestStream input" in {
    val testStream = testStreamOf[Array[Byte]]
      .addElements("a1".getBytes, "a2".getBytes)
      .advanceWatermarkToInfinity()

    the[PipelineExecutionException] thrownBy {
      JobTest[ReadAllBytesJob.type]
        .args("--input=in.txt", "--output=out.txt")
        .input(TextIO("in.txt"), Seq("a"))
        .inputStream(ReadIO("a"), testStream)
        .output(TextIO("out.txt")) { _ => }
        .run()
    } should have message
      s"java.lang.UnsupportedOperationException: Test input TestStream(${testStream.getEvents}) " +
      s"can't be converted to Iterable[T] to test this ScioIO type"
  }

  // =======================================================================
  // Handling incorrect test wiring
  // =======================================================================

  it should "fail missing test input" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message "requirement failed: Missing test input: TextIO(in.txt), available: []"
  }

  it should "fail misspelled test input" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("bad-in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message
      "requirement failed: Missing test input: TextIO(in.txt), available: [TextIO(bad-in.txt)]"
  }

  it should "fail unmatched test input" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .input(TextIO("unmatched.txt"), Seq("X", "Y"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message "requirement failed: Unmatched test input: TextIO(unmatched.txt)"
  }

  it should "fail duplicate test input" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .input(TextIO("in.txt"), Seq("X", "Y"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message "requirement failed: Duplicate test input: TextIO(in.txt)"
  }

  it should "fail missing test output" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .run()
    } should have message "requirement failed: Missing test output: TextIO(out.txt), available: []"
  }

  it should "fail misspelled test output" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .output(TextIO("bad-out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .run()
    } should have message
      "requirement failed: Missing test output: TextIO(out.txt), available: [TextIO(bad-out.txt)]"
  }

  it should "fail unmatched test output" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .output(TextIO("unmatched.txt"))(coll => coll should containInAnyOrder(Seq("X", "Y")))
        .run()
    } should have message "requirement failed: Unmatched test output: TextIO(unmatched.txt)"
  }

  it should "fail duplicate test output" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(Seq("X", "Y")))
        .run()
    } should have message "requirement failed: Duplicate test output: TextIO(out.txt)"
  }

  it should "fail missing test dist cache" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message
      "requirement failed: Missing test dist cache: DistCacheIO(dc.txt), available: []"
  }

  it should "fail misspelled test dist cache" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("bad-dc.txt"), Seq("1", "2"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message
      "requirement failed: Missing test dist cache: DistCacheIO(dc.txt), available: " +
      "[DistCacheIO(bad-dc.txt)]"
  }

  it should "fail unmatched test dist cache" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type]
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .distCache(DistCacheIO("unmatched.txt"), Seq("X", "Y"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message
      "requirement failed: Unmatched test dist cache: DistCacheIO(unmatched.txt)"
  }

  it should "fail duplicate test dist cache" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[DistCacheJob.type](enforceRun = false)
        .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
        .input(TextIO("in.txt"), Seq("a", "b"))
        .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
        .distCache(DistCacheIO("dc.txt"), Seq("X", "Y"))
        .output(TextIO("out.txt")) { coll =>
          coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
        }
        .run()
    } should have message
      "requirement failed: Duplicate test dist cache: DistCacheIO(dc.txt)"
  }

  it should "ignore materialize" in {
    JobTest[MaterializeJob.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(Seq("a", "b")))
      .run()
  }

  it should "fail job without close" in {
    the[IllegalArgumentException] thrownBy {
      JobTest[JobWithoutClose.type]
        .args("--output=out.avro")
        .output(ObjectFileIO[Long]("out.avro"))(coll => coll should containInAnyOrder(Seq(10L)))
        .run()
    } should have message
      "requirement failed: ScioContext was not executed. Did you forget .run()?"
  }

  // =======================================================================
  // Tests of JobTest testing wiring
  // =======================================================================

  class JobTestFromType extends PipelineSpec {
    "JobTestFromType" should "work" in {
      JobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
        .output(ObjectFileIO[Int]("out.avro")) { coll =>
          coll should containInAnyOrder(Seq(1, 2, 3))
        }
    }
  }

  class JobTestFromString extends PipelineSpec {
    "JobTestFromString" should "work" in {
      JobTest("com.spotify.scio.testing.ObjectFileJob")
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
        .output(ObjectFileIO[Int]("out.avro")) { coll =>
          coll should containInAnyOrder(Seq(1, 2, 3))
        }
    }
  }

  class MultiJobTest extends PipelineSpec {
    "MultiJobTest" should "work" in {
      JobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
        .output(ObjectFileIO[Int]("out.avro")) { coll =>
          coll should containInAnyOrder(Seq(1, 2, 3))
        }

      JobTest[ObjectFileJob.type]
        .args("--input=in2.avro", "--output=out2.avro")
        .input(ObjectFileIO[Int]("in2.avro"), Seq(1, 2, 3))
        .output(ObjectFileIO[Int]("out2.avro")) { coll =>
          coll should containInAnyOrder(Seq(1, 2, 3))
        }
    }
  }

  class OriginalJobTest extends PipelineSpec {
    import com.spotify.scio.testing.{JobTest => InternalJobTest}
    "OriginalJobTest" should "work" in {
      InternalJobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
        .output(ObjectFileIO[Int]("out.avro")) { coll =>
          coll should containInAnyOrder(Seq(1, 2, 3))
        }
    }
  }

  class JobTestWithNonUnitAssertion extends PipelineSpec {
    import com.spotify.scio.testing.{JobTest => InternalJobTest}
    "JobTestWithNonUnitAssertion" should "work" in {
      InternalJobTest[ObjectFileJob.type]
        .args("--input=in.avro", "--output=out.avro")
        .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
        // warns given flag `-Ywarn-value-discard`
        // if `output` only accepts `SCollection[_] => Unit`
        // instead of `SCollection[_] => Assertion`
        .output(ObjectFileIO[Int]("out.avro"))(_ should containInAnyOrder(Seq(1, 2, 3)))
    }
  }

  private val runMissedMessage =
    """|- should work \*\*\* FAILED \*\*\*
                                    |  Did you forget run\(\)\?
                                    |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                                    |  	args: --input=in.avro --output=out.avro
                                    |  	distCache: Map\(\)
                                    |  	inputs: ObjectFileIO\(in.avro\) -> List\(1, 2, 3\) \(JobTestTest.scala:.*\)""".stripMargin

  it should "enforce run() on JobTest from class type" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new JobTestFromType()
        .execute("JobTestFromType should work", color = false)
    }
    stdOutMock.message.mkString("") should include regex runMissedMessage
  }

  it should "enforce run() on multi JobTest" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new MultiJobTest().execute("MultiJobTest should work", color = false)
    }

    val msg =
      """|- should work \*\*\* FAILED \*\*\*
                 |  Did you forget run\(\)\?
                 |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                 |  	args: --input=in.avro --output=out.avro
                 |  	distCache: Map\(\)
                 |  	inputs: ObjectFileIO\(in.avro\) -> List\(1, 2, 3\)
                 |  Missing run\(\): JobTest\[com.spotify.scio.testing.ObjectFileJob\]\(
                 |  	args: --input=in2.avro --output=out2.avro
                 |  	distCache: Map\(\)
                 |  	inputs: ObjectFileIO\(in2.avro\) -> List\(1, 2, 3\) \(JobTestTest.scala:.*\)""".stripMargin

    stdOutMock.message.mkString("") should include regex msg
  }

  it should "enforce run() on JobTest from string class" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new JobTestFromString()
        .execute("JobTestFromString should work", color = false)
    }
    stdOutMock.message.mkString("") should include regex runMissedMessage
  }

  it should "not enforce run() on internal JobTest" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      new OriginalJobTest()
        .execute("OriginalJobTest should work", color = false)
    }
    stdOutMock.message.mkString("") shouldNot include regex runMissedMessage
  }

  // =======================================================================
  // Test invalid ScioIO
  // =======================================================================

  "runWithContext" should "fail input with message" in {
    val msg =
      "requirement failed: Missing test data. Are you reading input outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.textFile("in.txt"))
    } should have message msg
  }

  it should "fail output with message" in {
    val msg =
      "requirement failed: Missing test data. Are you writing output outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.parallelize(1 to 10).materialize)
    } should have message msg
  }

  it should "fail dist cache with message" in {
    val msg =
      "requirement failed: Missing test data. Are you using dist cache outside of JobTest?"
    the[IllegalArgumentException] thrownBy {
      runWithContext(_.distCache("in.txt")(f => Source.fromFile(f).getLines().toSeq))
    } should have message msg
  }

  it should "fail on duplicate usages of inputs in the job itself" in {
    val msg = "requirement failed: Test input TextIO(input) has already been read from once."
    the[IllegalArgumentException] thrownBy {
      JobTest[JobWithDuplicateInput.type]
        .args("--input=input")
        .input(TextIO("input"), Seq("does", "not", "matter"))
        .run()
    } should have message msg
  }

  it should "fail on duplicate outputs in the job itself" in {
    val msg = "requirement failed: Test output TextIO(output) has already been written to once."
    the[IllegalArgumentException] thrownBy {
      JobTest[JobWithDuplicateOutput.type]
        .args("--output=output")
        .output(TextIO("output"))(coll => coll should containSingleValue("does not matter"))
        .run()
    } should have message msg
  }

  // =======================================================================
  // Test metrics
  // =======================================================================

  it should "pass correct metrics test" in {
    JobTest[MetricsJob.type]
      .counter(MetricsJob.counter)(x => x shouldBe 10)
      .counters(_ should contain(MetricsJob.counter.getName -> 10))
      .distribution(MetricsJob.distribution) { d =>
        d.getCount shouldBe 10
        d.getMin shouldBe 1
        d.getMax shouldBe 10
        d.getSum shouldBe 55
        d.getMean shouldBe 5.5
      }
      .distributions(
        _ should contain(
          MetricsJob.distribution.getName ->
            DistributionResult.create(55, 10, 1, 10)
        )
      )
      .gauge(MetricsJob.gauge) { g =>
        g.getValue should be >= 1L
        g.getValue should be <= 10L
      }
      .gauges(_.map { case (_, result) =>
        result.getValue should be >= 1L
        result.getValue should be <= 10L
      })
      .run()
  }

  it should "fail incorrect counter test" in {
    the[TestFailedException] thrownBy {
      JobTest[MetricsJob.type]
        .counter(MetricsJob.counter)(x => x shouldBe 100)
        .run()
    } should have message "10 was not equal to 100"
  }

  it should "fail incorrect distribution test" in {
    the[TestFailedException] thrownBy {
      JobTest[MetricsJob.type]
        .distribution(MetricsJob.distribution)(x => x.getMax shouldBe 100)
        .run()
    } should have message "10 was not equal to 100"
  }

  it should "fail incorrect gauge test" in {
    val e = the[TestFailedException] thrownBy {
      JobTest[MetricsJob.type]
        .gauge(MetricsJob.gauge)(x => x.getValue should be >= 100L)
        .run()
    }
    e.getMessage should endWith(" was not greater than or equal to 100")
  }
}
