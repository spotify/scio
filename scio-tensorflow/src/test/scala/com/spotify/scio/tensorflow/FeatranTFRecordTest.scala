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

package com.spotify.scio.tensorflow

import java.nio.file.Files

import com.spotify.scio._
import com.spotify.scio.testing.PipelineSpec
import org.tensorflow.example.Example
import org.tensorflow.metadata.v0.{FixedShape, Schema, SparseFeature}

case class TrainingPoint(x1: Double, label: Double)

object ExamplesJobV2 {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(MetadataSchemaTest.examples)
      .saveAsTfExampleFile(args("output"))
    sc.close()
  }
}

object ExamplesJobV2WithSchema {
  def dummySchema(): Schema = {
    val schema = Schema.newBuilder()
    schema.addSparseFeature(
      SparseFeature.newBuilder
        .setName("sparseFeature")
        .setDenseShape(FixedShape.newBuilder.addDim(FixedShape.Dim.newBuilder().setSize(1)))
        .setValueFeature(SparseFeature.ValueFeature.newBuilder.setName("values"))
        .addIndexFeature(SparseFeature.IndexFeature.newBuilder.setName("indices")
        )).build()
    schema.build()
  }

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val examples = sc.parallelize(MetadataSchemaTest.examples)
    examples.saveAsTfExampleFile(args("output"), dummySchema())
    sc.close()
  }
}

class FeatranTFRecordTest extends PipelineSpec {

  "FeatranTFRecordSpec.normalizeName" should "work" in {
    FeatranTFRecordSpec.normalizeName("foo") shouldBe "foo"
    FeatranTFRecordSpec.normalizeName("Foo") shouldBe "Foo"
    FeatranTFRecordSpec.normalizeName("foo bar") shouldBe "foo_bar"
    FeatranTFRecordSpec.normalizeName("foo-bar") shouldBe "foo_bar"
    FeatranTFRecordSpec.normalizeName("Foo-Bar") shouldBe "Foo_Bar"
    FeatranTFRecordSpec.normalizeName("foo.bar-baz ala &33*(") shouldBe "foo_bar_baz_ala__33__"
  }

  "ExamplesJobV2" should "work" in {
    JobTest[ExamplesJobV2.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should satisfy[Example](_.size == 2))
      .run()
  }

  "ExamplesJobV2WithCustomSchema" should "work" in {
    JobTest[ExamplesJobV2WithSchema.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should satisfy[Example](_.size == 2))
      .run()
  }

  "saveExampleMetadata" should "work" in {
    val f = Files.createTempDirectory("saveExampleMetadataTest").resolve("schema.pb")
    f.toFile.deleteOnExit()
    val sc = ScioContext()
    val schema = ExamplesJobV2WithSchema.dummySchema()
    TFExampleSCollectionFunctions.saveExampleMetadata(sc.parallelize(Some(schema)),
      f.toFile.getAbsolutePath)
    sc.close()
    Files.readAllBytes(f) shouldBe schema.toByteArray
  }

}
