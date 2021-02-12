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
import com.spotify.scio.CoreSysProps
import org.apache.beam.sdk.io.LocalResources
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

object ParquetTypeFileOperationsTest {
  case class User(name: String, age: Int)
  val users = (1 to 10).map(i => User(s"user$i", i))

  case class Username(name: String)
}

class ParquetTypeFileOperationsTest extends AnyFlatSpec with Matchers {
  import ParquetTypeFileOperationsTest._

  def tmpDir = new File(new File(CoreSysProps.TmpDir.value), "scio-test-" + UUID.randomUUID())

  "ParquetTypeFileOperations" should "work" in {
    val dir = tmpDir
    val file = LocalResources
      .fromFile(dir, true)
      .resolve("file.parquet", StandardResolveOptions.RESOLVE_FILE)
    writeFile(file)

    val fileOps = ParquetTypeFileOperations[User]()
    val actual = fileOps.iterator(file).asScala.toSeq

    actual shouldBe users
    tmpDir.delete()
  }

  it should "work with projection" in {
    val dir = tmpDir
    val file = LocalResources
      .fromFile(dir, true)
      .resolve("file.parquet", StandardResolveOptions.RESOLVE_FILE)
    writeFile(file)

    val fileOps = ParquetTypeFileOperations[Username]()
    val actual = fileOps.iterator(file).asScala.toSeq

    actual shouldBe users.map(u => Username(u.name))
    tmpDir.delete()
  }

  it should "work with predicate" in {
    val dir = tmpDir
    val file = LocalResources
      .fromFile(dir, true)
      .resolve("file.parquet", StandardResolveOptions.RESOLVE_FILE)
    writeFile(file)

    val predicate = FilterApi.ltEq(FilterApi.intColumn("age"), java.lang.Integer.valueOf(5))
    val fileOps = ParquetTypeFileOperations[User](predicate)
    val actual = fileOps.iterator(file).asScala.toSeq

    actual shouldBe users.filter(_.age <= 5)
    tmpDir.delete()
  }

  private def writeFile(file: ResourceId): Unit = {
    val fileOps = ParquetTypeFileOperations[User](CompressionCodecName.GZIP)
    val writer = fileOps.createWriter(file);
    users.foreach(writer.write)
    writer.close()
  }
}
