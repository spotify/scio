/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.parquet.tensorflow

import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ClosedTap, FileNamePolicySpec, ScioIOTest, TapSpec}
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.Schema
import org.apache.commons.io.FileUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.scalatest.BeforeAndAfterAll
import org.tensorflow.proto.example.{BytesList, Example, Feature, Features, FloatList, Int64List}

import java.nio.file.Files
import scala.jdk.CollectionConverters._

object ParquetExampleHelper {
  private[tensorflow] val schema = {
    var builder = Schema.newBuilder()
    (1 to 5).foreach(i => builder = builder.required(s"int64_req_$i", Schema.Type.INT64))
    (1 to 5).foreach(i => builder = builder.required(s"float_req_$i", Schema.Type.FLOAT))
    (1 to 5).foreach(i => builder = builder.required(s"bytes_req_$i", Schema.Type.BYTES))
    (1 to 5).foreach(i => builder = builder.optional(s"int64_opt_$i", Schema.Type.INT64))
    (1 to 5).foreach(i => builder = builder.optional(s"float_opt_$i", Schema.Type.FLOAT))
    (1 to 5).foreach(i => builder = builder.optional(s"bytes_opt_$i", Schema.Type.BYTES))
    (1 to 5).foreach(i => builder = builder.repeated(s"int64_rep_$i", Schema.Type.INT64))
    (1 to 5).foreach(i => builder = builder.repeated(s"float_rep_$i", Schema.Type.FLOAT))
    (1 to 5).foreach(i => builder = builder.repeated(s"bytes_rep_$i", Schema.Type.BYTES))
    builder.named("Example")
  }

  private def longs(xs: Long*): Feature =
    Feature
      .newBuilder()
      .setInt64List(Int64List.newBuilder().addAllValue(xs.asInstanceOf[Seq[java.lang.Long]].asJava))
      .build()

  private def floats(xs: Float*): Feature =
    Feature
      .newBuilder()
      .setFloatList(
        FloatList.newBuilder().addAllValue(xs.asInstanceOf[Seq[java.lang.Float]].asJava)
      )
      .build()

  private def bytes(xs: String*): Feature =
    Feature
      .newBuilder()
      .setBytesList(BytesList.newBuilder().addAllValue(xs.map(ByteString.copyFromUtf8).asJava))
      .build()

  private[tensorflow] def newExample(i: Int): Example = {
    var builder = Features.newBuilder()
    (1 to 5).foreach(i => builder = builder.putFeature(s"int64_req_$i", longs(i.toLong)))
    (1 to 5).foreach(i => builder = builder.putFeature(s"float_req_$i", floats(i.toFloat)))
    (1 to 5).foreach(i => builder = builder.putFeature(s"bytes_req_$i", bytes(s"bytes$i")))
    if (i % 2 == 0) {
      (1 to 5).foreach(i => builder = builder.putFeature(s"int64_opt_$i", longs(i.toLong)))
      (1 to 5).foreach(i => builder = builder.putFeature(s"float_opt_$i", floats(i.toFloat)))
      (1 to 5).foreach(i => builder = builder.putFeature(s"bytes_opt_$i", bytes(s"bytes$i")))
    }
    (1 to 5).foreach(i =>
      builder = builder.putFeature(s"int64_rep_$i", longs(Seq.fill(5)(i.toLong): _*))
    )
    (1 to 5).foreach(i =>
      builder = builder.putFeature(s"float_rep_$i", floats(Seq.fill(5)(i.toFloat): _*))
    )
    (1 to 5).foreach(i =>
      builder = builder.putFeature(s"bytes_rep_$i", bytes(Seq.fill(5)(s"bytes$i"): _*))
    )
    Example.newBuilder().setFeatures(builder).build()
  }
}

class ParquetExampleIOFileNamePolicyTest extends FileNamePolicySpec[Example] {
  import ParquetExampleHelper._

  override val suffix: String = ".parquet"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[Example] = {
    in.map(newExample)
      .saveAsParquetExampleFile(
        tmpDir,
        schema,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else ScioIOTest.TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[Example]] = Seq(
    _.map(newExample).saveAsParquetExampleFile(
      "nonsense",
      schema,
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ParquetExampleIOTest extends ScioIOSpec with TapSpec with BeforeAndAfterAll {
  import ParquetExampleHelper._
  private val testDir = Files.createTempDirectory("scio-test-").toFile
  private val examples = (1 to 10).map(newExample)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc.parallelize(examples).saveAsParquetExampleFile(testDir.getAbsolutePath, schema)
    sc.run()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  private val projection = Seq(
    "int64_req_1",
    "float_req_1",
    "bytes_req_1",
    "int64_opt_1",
    "float_opt_1",
    "bytes_opt_1",
    "int64_rep_1",
    "float_rep_1",
    "bytes_rep_1"
  )

  private val predicate = FilterApi.and(
    FilterApi.ltEq(FilterApi.longColumn("int64_req_1"), java.lang.Long.valueOf(5L)),
    FilterApi.gtEq(FilterApi.floatColumn("float_req_2"), java.lang.Float.valueOf(2.5f))
  )

  private def projectFields(xs: Seq[String]): Example => Example = (e: Example) => {
    val m = e.getFeatures.getFeatureMap
    Example
      .newBuilder()
      .setFeatures(xs.foldLeft(Features.newBuilder()) { (b, f) =>
        val feature = m.get(f)
        if (feature == null) b else b.putFeature(f, feature)
      })
      .build()
  }

  "ParquetExampleIO" should "work" in {
    val xs = (1 to 100).map(newExample)
    testTap(xs)(_.saveAsParquetExampleFile(_, schema))(".parquet")
    testJobTest(xs)(ParquetExampleIO(_))(_.parquetExampleFile(_))(
      _.saveAsParquetExampleFile(_, schema)
    )
  }

  it should "read Examples" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = testDir.getAbsolutePath,
      suffix = ".parquet"
    )
    data should containInAnyOrder(examples)
    sc.run()
    ()
  }

  it should "read Examples with projection" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = testDir.getAbsolutePath,
      projection = projection,
      suffix = ".parquet"
    )
    data should containInAnyOrder(examples.map(projectFields(projection)))
    sc.run()
    ()
  }

  it should "read Examples with predicate" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = testDir.getAbsolutePath,
      predicate = predicate,
      suffix = ".parquet"
    )
    val expected = examples.filter { e =>
      e.getFeatures.getFeatureOrThrow("int64_req_1").getInt64List.getValue(0) <= 5L &&
      e.getFeatures.getFeatureOrThrow("float_req_2").getFloatList.getValue(0) >= 2.5f
    }
    data should containInAnyOrder(expected)
    sc.run()
    ()
  }

  it should "read Examples with projection and predicate in non-test context" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = testDir.getAbsolutePath,
      projection = projection,
      predicate = predicate,
      suffix = ".parquet"
    )
    val expected = examples
      .filter { e =>
        e.getFeatures.getFeatureOrThrow("int64_req_1").getInt64List.getValue(0) <= 5L &&
        e.getFeatures.getFeatureOrThrow("float_req_2").getFloatList.getValue(0) >= 2.5f
      }
      .map(projectFields(projection))
    data should containInAnyOrder(expected)
    sc.run()
    ()
  }

  it should "read Examples with projection in a JobTest context" in {
    val projected = examples.map(projectFields(projection))

    testJobTest(projected)(ParquetExampleIO(_))(_.parquetExampleFile(_, projection = projection))(
      _.saveAsParquetExampleFile(_, schema)
    )
  }
}
