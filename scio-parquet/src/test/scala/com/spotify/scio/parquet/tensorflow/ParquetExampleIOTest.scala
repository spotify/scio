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
import com.spotify.scio.parquet.{BeamInputFile, ParquetConfiguration}
import com.spotify.scio.parquet.types._
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.commons.io.FileUtils
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.ParquetFileReader
import org.scalatest.BeforeAndAfterAll
import org.tensorflow.metadata.{v0 => tfmd}
import org.tensorflow.proto.example._

import java.nio.file.Files
import scala.jdk.CollectionConverters._

object ParquetExampleHelper {

  private val required = tfmd.ValueCount.newBuilder().setMin(1).setMax(1).build()
  private val optional = tfmd.ValueCount.newBuilder().setMin(0).setMax(1).build()
  private val repeated = tfmd.ValueCount.newBuilder().setMin(1).setMax(4).build()

  final case class LegacyExampleParquet(
    int64_required: Long,
    int64_optional: Option[Long],
    int64_repeated: List[Long],
    int64_empty: List[Long],
    float_required: Float,
    float_optional: Option[Float],
    float_repeated: List[Float],
    float_empty: List[Float],
    bytes_required: String,
    bytes_optional: Option[String],
    bytes_repeated: List[String],
    bytes_empty: List[String]
  )
  implicit val ptLegacyExampleParquet: ParquetType[LegacyExampleParquet] =
    ParquetType[LegacyExampleParquet]

  // format: off
  private[tensorflow] val schema = tfmd.Schema.newBuilder()
    .addFeature(tfmd.Feature.newBuilder().setName("int64_required").setType(tfmd.FeatureType.INT).setValueCount(required).build())
    .addFeature(tfmd.Feature.newBuilder().setName("int64_optional").setType(tfmd.FeatureType.INT).setValueCount(optional).build())
    .addFeature(tfmd.Feature.newBuilder().setName("int64_repeated").setType(tfmd.FeatureType.INT).setValueCount(repeated).build())
    .addFeature(tfmd.Feature.newBuilder().setName("int64_empty").setType(tfmd.FeatureType.INT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_required").setType(tfmd.FeatureType.FLOAT).setValueCount(required).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_optional").setType(tfmd.FeatureType.FLOAT).setValueCount(optional).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_repeated").setType(tfmd.FeatureType.FLOAT).setValueCount(repeated).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_empty").setType(tfmd.FeatureType.FLOAT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_required").setType(tfmd.FeatureType.BYTES).setValueCount(required).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_optional").setType(tfmd.FeatureType.BYTES).setValueCount(optional).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_repeated").setType(tfmd.FeatureType.BYTES).setValueCount(repeated).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_empty").setType(tfmd.FeatureType.BYTES).build())
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
    val long = i.toLong
    val float = i.toFloat
    val str = i.toString

    // parquet limitation. At read time, we can't disambiguate
    // empty tensor from missing feature (both considered as missing)
    val features = Features.newBuilder()
      .putFeature("int64_required", longs(long))
      .putFeature("int64_repeated", longs(long, long, long))
      // .putFeature("int64_empty", longs())
      .putFeature("float_required", floats(float))
      .putFeature("float_repeated", floats(float, float, float))
      // .putFeature("float_empty", floats())
      .putFeature("bytes_required", bytes(str))
      .putFeature("bytes_repeated", bytes(str, str, str))
      // .putFeature("bytes_empty", bytes())
      .build()
    Example.newBuilder().setFeatures(features).build()
  }

  private[tensorflow] def newLegacy(i: Int): LegacyExampleParquet = {
    val long = i.toLong
    val float = i.toFloat
    val str = i.toString
    LegacyExampleParquet(
      int64_required = long,
      int64_optional = None,
      int64_repeated = List(long, long, long),
      int64_empty = List.empty,
      float_required = float,
      float_optional = None,
      float_repeated = List(float, float, float),
      float_empty = List.empty,
      bytes_required = str,
      bytes_optional = None,
      bytes_repeated = List(str, str, str),
      bytes_empty = List.empty
    )
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
  private val testDir = Files.createTempDirectory("scio-test-")
  private val currentDir = testDir.resolve("current").toFile
  private val legacyDir = testDir.resolve("legacy").toFile
  private val examples = (1 to 10).map(newExample)

  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    val coll = sc.parallelize(1 to 10)
    coll.map(newExample).saveAsParquetExampleFile(currentDir.getAbsolutePath, schema)

    // legacy: old saveAsParquetExampleFile schema included field repetition
    // make sure we can still read parquet file with non-repeated fields
    coll.map(newLegacy).saveAsTypedParquetFile(legacyDir.getAbsolutePath)
    sc.run()
  }

  override protected def afterAll(): Unit = FileUtils.deleteDirectory(testDir.toFile)

  // format: off
  private val projection = tfmd.Schema
    .newBuilder()
    .addFeature(tfmd.Feature.newBuilder().setName("int64_required").setType(tfmd.FeatureType.INT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("float_required").setType(tfmd.FeatureType.FLOAT).build())
    .addFeature(tfmd.Feature.newBuilder().setName("bytes_required").setType(tfmd.FeatureType.BYTES).build())
    .build()

  private val predicate = FilterApi.and(
    FilterApi.ltEq(FilterApi.longColumn("int64_required"), java.lang.Long.valueOf(5L)),
    FilterApi.gtEq(FilterApi.floatColumn("float_required"), java.lang.Float.valueOf(2.5f))
  )
  // format: on

  private def projectFields(projection: tfmd.Schema): Example => Example = { (e: Example) =>
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
      path = currentDir.getAbsolutePath,
      suffix = ".parquet"
    )
    data should containInAnyOrder(examples)
    sc.run()
    ()
  }

  it should "read Examples with projection" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = currentDir.getAbsolutePath,
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
      path = currentDir.getAbsolutePath,
      predicate = predicate,
      suffix = ".parquet"
    )
    val expected = examples.filter { e =>
      e.getFeatures.getFeatureOrThrow("int64_required").getInt64List.getValue(0) <= 5L &&
      e.getFeatures.getFeatureOrThrow("float_required").getFloatList.getValue(0) >= 2.5f
    }
    data should containInAnyOrder(expected)
    sc.run()
    ()
  }

  it should "read Examples from legacy example parquet file" in {
    val sc = ScioContext()
    val data = sc.parquetExampleFile(
      path = legacyDir.getAbsolutePath,
      suffix = ".parquet"
    )
    data should containInAnyOrder(examples)
    sc.run()
    ()
  }

  it should "read Examples with projection in a JobTest context" in {
    val projected = examples.map(projectFields(projection))

    testJobTest(projected)(ParquetExampleIO(_))(_.parquetExampleFile(_, projection = projection))(
      _.saveAsParquetExampleFile(_, schema)
    )
  }

  it should "write extra metadata" in withTempDir { dir =>
    val sc = ScioContext()
    val outDir = s"${dir.toPath.resolve("test-metadata").toFile.getAbsolutePath}"

    sc
      .parallelize(1 to 10)
      .map(newExample)
      .saveAsParquetExampleFile(
        outDir,
        schema,
        metadata = Map("foo" -> "bar", "bar" -> "baz"),
        numShards = 1
      )
    sc.run()

    val options = HadoopReadOptions.builder(ParquetConfiguration.empty()).build
    val r =
      ParquetFileReader.open(BeamInputFile.of(s"$outDir/part-00000-of-00001.parquet"), options)
    val metadata = r.getFileMetaData.getKeyValueMetaData

    metadata.get("foo") shouldBe "bar"
    metadata.get("bar") shouldBe "baz"

    r.close()
  }
}
