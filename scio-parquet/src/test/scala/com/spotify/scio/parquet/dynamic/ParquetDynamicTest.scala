/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.parquet.dynamic

import java.nio.file.{Files, Path}
import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.apache.commons.io.FileUtils

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.Random

case class TypedRecord(
  int: Int,
  long: Long,
  float: Float,
  bool: Boolean,
  string: String,
  stringList: List[String]
)

trait ParquetDynamicTest extends PipelineSpec {
  val aNames: Seq[String] = Seq("Anna", "Alice", "Albrecht", "Amy", "Arlo", "Agnes")
  val bNames: Seq[String] = Seq("Bob", "Barbara", "Barry", "Betty", "Brody", "Bard")

  private def verifyOutput(path: Path, expected: String*): Unit = {
    val actual = Files
      .list(path)
      .iterator()
      .asScala
      .filterNot(_.toFile.getName.startsWith("."))
      .toSet
    actual shouldBe expected.map(path.resolve).toSet
    ()
  }

  def dynamicTest[T: Coder](
    input: Seq[T],
    save: (SCollection[T], Path) => ClosedTap[_],
    read: (ScioContext, Path) => (SCollection[String], SCollection[String])
  ): Unit = {
    val tmpDir = Files.createTempDirectory("parquet-dynamic-io-")
    val sc = ScioContext()
    save(sc.parallelize(input), tmpDir)
    sc.run()
    verifyOutput(tmpDir, "0", "1")

    val sc2 = ScioContext()
    val (lines0, lines1) = read(sc2, tmpDir)

    lines0 should containInAnyOrder(aNames)
    lines1 should containInAnyOrder(bNames)
    sc2.run()
    FileUtils.deleteDirectory(tmpDir.toFile)
  }
}

class ParquetTensorflowDynamicTest extends ParquetDynamicTest {
  import com.google.protobuf.ByteString
  import me.lyh.parquet.tensorflow.Schema
  import org.tensorflow.proto.example._
  import com.spotify.scio.parquet.tensorflow._
  import com.spotify.scio.parquet.tensorflow.dynamic._

  def tfRec(int: Int, name: String): Example = {
    def f(name: String)(fn: Feature.Builder => Feature.Builder): (String, Feature) =
      (name, fn(Feature.newBuilder()).build)

    val features = Map[String, Feature](
      f("ints") { fb =>
        val vals = Int64List.newBuilder()
        List(int).foreach(i => vals.addValue(i.toLong))
        fb.setInt64List(vals)
      },
      f("floats") { fb =>
        val vals = FloatList.newBuilder()
        Seq(1.0f).foreach(vals.addValue)
        fb.setFloatList(vals)
      },
      f("strings") { fb =>
        val vals = BytesList.newBuilder()
        List(name).foreach(s => vals.addValue(ByteString.copyFromUtf8(s)))
        fb.setBytesList(vals)
      }
    )
    Example
      .newBuilder()
      .setFeatures(Features.newBuilder().putAllFeature(features.asJava))
      .build
  }
  val input: Seq[Example] = Random.shuffle(aNames.map(tfRec(0, _)) ++ bNames.map(tfRec(1, _)))

  it should "support Parquet Example files" in {
    val schema = {
      val builder = Schema.newBuilder()
      builder.required(s"ints", Schema.Type.INT64)
      builder.required(s"floats", Schema.Type.FLOAT)
      builder.required(s"strings", Schema.Type.BYTES)
      builder.named("Example")
    }

    val getStr: Example => String = _.getFeatures
      .getFeatureOrThrow("strings")
      .getBytesList
      .getValue(0)
      .toString(StandardCharsets.UTF_8)

    dynamicTest[Example](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicParquetExampleFile(tmpDir.toAbsolutePath.toString, schema) { t =>
          s"${t.getFeatures.getFeatureOrThrow("ints").getInt64List.getValue(0)}"
        },
      (sc2, tmpDir) => {
        val lines0 = sc2.parquetExampleFile(s"$tmpDir/0/*.parquet").map(getStr)
        val lines1 = sc2.parquetExampleFile(s"$tmpDir/1/*.parquet").map(getStr)
        (lines0, lines1)
      }
    )
  }
}

class ParquetTypedDynamicTest extends ParquetDynamicTest {
  import com.spotify.scio.parquet.types._
  import com.spotify.scio.parquet.types.dynamic._

  def typedRec(int: Int, name: String): TypedRecord =
    TypedRecord(int, 0L, 0f, false, name, List("a"))
  val input: Seq[TypedRecord] =
    Random.shuffle(aNames.map(n => typedRec(0, n)) ++ bNames.map(n => typedRec(1, n)))

  it should "support typed Parquet files" in {
    dynamicTest[TypedRecord](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicTypedParquetFile(tmpDir.toAbsolutePath.toString)(t => s"${t.int}"),
      (sc2, tmpDir) => {
        val lines0 = sc2.typedParquetFile[TypedRecord](s"$tmpDir/0/*.parquet").map(_.string)
        val lines1 = sc2.typedParquetFile[TypedRecord](s"$tmpDir/1/*.parquet").map(_.string)
        (lines0, lines1)
      }
    )
  }
}

class ParquetAvroDynamicTest extends ParquetDynamicTest {
  import com.spotify.scio.avro._
  import com.spotify.scio.parquet.avro._
  import com.spotify.scio.parquet.avro.dynamic._

  def avroRec(int: Int, name: String): TestRecord =
    new TestRecord(int, 0L, 0f, 1000.0, false, name, List[CharSequence]("a").asJava)

  val input: Seq[TestRecord] =
    Random.shuffle(aNames.map(n => avroRec(0, n)) ++ bNames.map(n => avroRec(1, n)))

  it should "support Parquet Avro files" in {
    dynamicTest[TestRecord](
      input,
      (scoll, tmpDir) =>
        scoll.saveAsDynamicParquetAvroFile(tmpDir.toAbsolutePath.toString) { t =>
          s"${t.int_field}"
        },
      (sc2, tmpDir) => {
        val lines0 =
          sc2.parquetAvroFile[TestRecord](s"$tmpDir/0/*.parquet").map(_.getStringField.toString)
        val lines1 =
          sc2.parquetAvroFile[TestRecord](s"$tmpDir/1/*.parquet").map(_.getStringField.toString)
        (lines0, lines1)
      }
    )
  }
}
