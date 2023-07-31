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
import org.apache.commons.io.FileUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.scalatest.BeforeAndAfterAll
import org.tensorflow.metadata.{v0 => tfmd}
import org.tensorflow.proto.example.{BytesList, Example, Feature, Features, FloatList, Int64List}

import java.nio.file.Files
import scala.jdk.CollectionConverters._

object ParquetExampleHelper {
  // format: off
  private[tensorflow] val schema = tfmd.Schema.newBuilder()
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"int64_req_$i").setType(tfmd.FeatureType.INT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"float_req_$i").setType(tfmd.FeatureType.FLOAT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"bytes_req_$i").setType(tfmd.FeatureType.BYTES).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"int64_opt_$i").setType(tfmd.FeatureType.INT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"float_opt_$i").setType(tfmd.FeatureType.FLOAT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"bytes_opt_$i").setType(tfmd.FeatureType.BYTES).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"int64_rep_$i").setType(tfmd.FeatureType.INT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"float_rep_$i").setType(tfmd.FeatureType.FLOAT).build()).asJava)
    .addAllFeature((1 to 5).map(i => tfmd.Feature.newBuilder().setName(s"bytes_rep_$i").setType(tfmd.FeatureType.BYTES).build()).asJava)
    .build()
  // format: on

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

  // format: off
  private[tensorflow] def newExample(i: Int): Example = {
    val features = Features.newBuilder()
      .putAllFeature((1 to 5).map(i => s"int64_req_$i" -> longs(i.toLong)).toMap.asJava)
      .putAllFeature((1 to 5).map(i => s"float_req_$i" -> floats(i.toFloat)).toMap.asJava)
      .putAllFeature((1 to 5).map(i => s"bytes_req_$i" -> bytes(s"bytes$i")).toMap.asJava)
      .putAllFeature((1 to 5).filter(_ % 2 == 0).map(i => s"int64_opt_$i" -> longs(i.toLong)).toMap.asJava)
      .putAllFeature((1 to 5).filter(_ % 2 == 0).map(i => s"float_opt_$i" -> floats(i.toFloat)).toMap.asJava)
      .putAllFeature((1 to 5).filter(_ % 2 == 0).map(i => s"bytes_opt_$i" -> bytes(s"bytes$i")).toMap.asJava)
      .putAllFeature((1 to 5).map(i => s"int64_rep_$i" -> longs(Seq.fill(5)(i.toLong): _*)).toMap.asJava)
      .putAllFeature((1 to 5).map(i => s"float_rep_$i" -> floats(Seq.fill(5)(i.toFloat): _*)).toMap.asJava)
      .putAllFeature((1 to 5).map(i => s"bytes_rep_$i" -> bytes(Seq.fill(5)(s"bytes$i"): _*)).toMap.asJava)
      .build()
    Example.newBuilder().setFeatures(features).build()
  }
  // format: on
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

  // format: off
  private val projection = tfmd.Schema
    .newBuilder()
    .addFeature(tfmd.Feature.newBuilder().setName("int64_req_1").setType(tfmd.FeatureType.INT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_req_1").setType(tfmd.FeatureType.FLOAT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_req_1").setType(tfmd.FeatureType.BYTES).build())
    .addFeature(tfmd.Feature.newBuilder().setName("int64_opt_1").setType(tfmd.FeatureType.INT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_opt_1").setType(tfmd.FeatureType.FLOAT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_opt_1").setType(tfmd.FeatureType.BYTES).build())
    .addFeature(tfmd.Feature.newBuilder().setName("int64_rep_1").setType(tfmd.FeatureType.INT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_rep_1").setType(tfmd.FeatureType.FLOAT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_rep_1").setType(tfmd.FeatureType.BYTES).build())
    .build()
  // format: on

  private def projectFields(projection: tfmd.Schema): Example => Example = (e: Example) => {
    val m = e.getFeatures.getFeatureMap.asScala
    Example
      .newBuilder()
      .setFeatures(projection.getFeatureList.asScala.foldLeft(Features.newBuilder()) { (b, f) =>
        m.get(f.getName).fold(b)(b.putFeature(f.getName, _))
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

  it should "read Examples with projection in a JobTest context" in {
    val projected = examples.map(projectFields(projection))

    testJobTest(projected)(ParquetExampleIO(_))(_.parquetExampleFile(_, projection = projection))(
      _.saveAsParquetExampleFile(_, schema)
    )
  }
}
