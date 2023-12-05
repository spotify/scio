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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedAvroIT extends AnyFlatSpec with Matchers {
  "Typed Avro IO" should "not throw exception" in {
    noException should be thrownBy {
      val tempDir = Files.createTempDir()
      TypedAvroJob.main(Array("--runner=DirectRunner", s"--output=${tempDir.getAbsolutePath}"))
      tempDir.deleteOnExit()
    }
  }
}

object TypedAvroJob {
  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    sc.parallelize(Seq("a", "b", "c"))
      .map(Record(_))
      .saveAsTypedAvroFile(args("output"))
    sc.run().waitUntilDone()
    ()
  }

  @AvroType.toSchema
  case class Record(s: String)
}
