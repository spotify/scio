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

package com.spotify.scio.avro.types

import java.nio.file.Files

import com.spotify.scio._
import com.spotify.scio.avro._
import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConverterProviderTest extends AnyFlatSpec with Matchers {
  import ConverterProviderTest._

  "ConverterProvider" should "#1831: handle Avro map" in {
    val dir = Files.createTempDirectory("avro-").toFile
    val data = Seq(Record(Map("a" -> 1), Some(Map("b" -> 2)), List(Map("c" -> 3))))

    val sc1 = ScioContext()
    sc1.parallelize(data).saveAsTypedAvroFile(dir.getAbsolutePath)
    sc1.run()

    val sc2 = ScioContext()
    val t = sc2.typedAvroFile[Record](dir.getAbsolutePath, ".avro").materialize
    sc2.run()

    t.underlying.value.toSeq should contain theSameElementsAs data

    FileUtils.deleteDirectory(dir)
  }
}

object ConverterProviderTest {
  @AvroType.toSchema
  case class Record(a: Map[String, Int], b: Option[Map[String, Int]], c: List[Map[String, Int]])
}
