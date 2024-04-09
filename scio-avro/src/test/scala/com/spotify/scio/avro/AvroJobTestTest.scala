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

package com.spotify.scio.avro

import com.spotify.scio._
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.generic.GenericRecord

object ObjectFileJob {

  // #JobTestTest_io_pipeline_section
  def pipeline(sc: ScioContext, input: String, output: String): Unit = {
    sc.objectFile[Int](input)
      .map(_ * 10)
      .saveAsObjectFile(output)
  }
  // #JobTestTest_io_pipeline_section

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    pipeline(sc, args("input"), args("output"))
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
    sc.avroFile(args("input"), AvroUtils.schema)
      .saveAsAvroFile(args("output"), schema = AvroUtils.schema)
    sc.run()
    ()
  }
}

object GenericParseFnAvroFileJob {

  implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(AvroUtils.schema)

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

class AvroJobTestTest extends PipelineSpec {
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
    an[AssertionError] should be thrownBy {
      testObjectFileJob(10, 20)
    }
    an[AssertionError] should be thrownBy {
      testObjectFileJob(10, 20, 30, 40)
    }
  }

  it should "execute anonymous job" in {
    import ObjectFileJob.pipeline
    // #JobTestTest_anonymous_job_test
    JobTest(pipeline(_, "in.avro", "out.avro"))
      .input(ObjectFileIO[Int]("in.avro"), Seq(1, 2, 3))
      .output(ObjectFileIO[Int]("out.avro"))(_ should containInAnyOrder(Seq(10, 20, 30)))
      .run()
    // #JobTestTest_anonymous_job_test
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
    implicit val coder = avroGenericRecordCoder
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
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder
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
}
