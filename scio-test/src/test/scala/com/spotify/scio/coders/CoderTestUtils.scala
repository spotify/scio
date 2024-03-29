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

package com.spotify.scio.coders

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.options.ScioOptions
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.util.CoderUtils

import java.io.{File, FileOutputStream}
import java.nio.file.Files

object CoderTestUtils {
  case class Pair(name: String, size: Int)
  case class CaseClassWithGenericRecord(name: String, size: Int, record: GenericRecord)
  case class CaseClassWithSpecificRecord(name: String, size: Int, record: TestRecord)

  def testRoundTrip[T](coder: BCoder[T], value: T): Boolean =
    testRoundTrip(coder, coder, value)

  def testRoundTrip[T](writer: BCoder[T], reader: BCoder[T], value: T): Boolean = {
    val bytes = CoderUtils.encodeToByteArray(writer, value)
    val result = CoderUtils.decodeFromByteArray(reader, bytes)
    result == value
  }

  case class ZstdTestCaseClass(a: Int, b: String, c: Long)

  def writeZstdBytes(bytes: Array[Byte]): File = {
    val tmp = Files.createTempFile("zstd-test", ".bin").toFile
    tmp.deleteOnExit()
    val fos = new FileOutputStream(tmp)
    try {
      fos.write(bytes)
    } finally {
      fos.close()
    }
    tmp
  }

  def zstdOpts(className: String, path: String): ScioOptions = {
    val (opts, _) = ScioContext.parseArguments[ScioOptions](
      Array(
        s"--zstdDictionary=com.spotify.scio.coders.CoderTestUtils$$${className}:${path}"
      )
    )
    opts
  }
}
