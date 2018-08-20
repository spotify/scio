/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.nio

import java.io.File
import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.io.Tap
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils

import scala.concurrent.Future
import scala.reflect.ClassTag

object NioIT {
  case class Record(i: Int, s: String, r: List[String])
}

class NioIT extends PipelineSpec {

  private def testIO[T: ClassTag](xs: Seq[T])
                                 (ioFn: String => ScioIO[T])
                                 (readFn: (ScioContext, String) => SCollection[T])
                                 (writeFn: (SCollection[T], String) => Future[Tap[T]]): Unit = {
    val tmpDir = new File(
      new File(sys.props("java.io.tmpdir")),
      "scio-test-" + UUID.randomUUID())

    val sc1 = ScioContext()
    val data = sc1.parallelize(xs)
    val future = writeFn(data, tmpDir.getAbsolutePath)
    sc1.close().waitUntilDone()
    val tap = future.waitForResult()
    tap.value.toSeq should contain theSameElementsAs xs

    val sc2 = ScioContext()
    tap.open(sc2) should containInAnyOrder (xs)
    FileUtils.deleteDirectory(tmpDir)

    def runMain(args: Array[String]): Unit = {
      val (sc, argz) = ContextAndArgs(args)
      val data = readFn(sc, argz("input"))
      writeFn(data, argz("output"))
      sc.close()
    }

    val builder = com.spotify.scio.testing.JobTest("null")
      .input(ioFn("in"), xs)
      .output(ioFn("out"))(_ should containInAnyOrder (xs))
    builder.setUp()
    runMain(Array("--input=in", "--output=out") :+ s"--appName=${builder.testId}")
    builder.tearDown()
  }

  "AvroIO" should "work with SpecificRecord" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testIO(xs)(AvroIO(_))(_.avroFile(_))(_.saveAsAvroFile(_))
  }

  it should "work with GenericRecord" in {
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    testIO(xs)(AvroIO(_))(
      _.avroFile(_, AvroUtils.schema))(
      _.saveAsAvroFile(_, schema = AvroUtils.schema))
  }

  "ObjectFileIO" should "work" in {
    import NioIT._
    val xs = (1 to 100).map(x => Record(x, x.toString, (1 to x).map(_.toString).toList))
    testIO(xs)(ObjectFileIO(_))(_.objectFile(_))(_.saveAsObjectFile(_))
  }

  "TextIO" should "work" in {
    val xs = (1 to 100).map(_.toString)
    testIO(xs)(TextIO(_))(_.textFile(_))(_.saveAsTextFile(_))
  }

}
