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

import com.spotify.featran.scio._
import com.spotify.featran.tensorflow._
import com.spotify.featran.transformers.Identity
import com.spotify.featran.{FeatureSpec, MultiFeatureSpec}
import com.spotify.scio._
import com.spotify.scio.testing.{PipelineSpec, ProtobufIO, TextIO}
import org.scalatest.Matchers
import org.tensorflow.example.Example
import org.tensorflow.metadata.v0.{FixedShape, Schema, SparseFeature}
import org.tensorflow.{example => tf}

case class TrainingPoint(x1: Double, label: Double)

object FeatureSpecJob {

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val featureSpec = FeatureSpec.of[TrainingPoint]
      .required(_.x1)(Identity("x.1"))
      .required(_.label)(Identity("label"))

    val collection = sc.textFile(args("input"))
      .map(_.split(","))
      .collect { case Array(x1, l) => TrainingPoint(x1.toDouble, l.toDouble) }

    val features = featureSpec.extract(collection)

    val (train, test) = features
      .featureValues[tf.Example]
      .randomSplit(.9)

    train.saveAsTfExampleFile(args("output") + "/train", features)
    test.saveAsTfExampleFile(args("output") + "/test", features)

    sc.close()
  }
}

object MultiSpecJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val features = FeatureSpec.of[TrainingPoint]
      .required(_.x1)(Identity("x1"))

    val label = FeatureSpec.of[TrainingPoint]
      .required(_.label)(Identity("label"))

    val collection = sc.textFile(args("input"))
      .map(_.split(","))
      .collect { case Array(x1, l) => TrainingPoint(x1.toDouble, l.toDouble) }

    val dataset = MultiFeatureSpec(features, label)
      .extract(collection)

    val (train, test) = dataset
      .featureValues[tf.Example]
      .randomSplit(.9)

    train.saveAsTfExampleFile(args("output") + "/train", dataset)
    test.saveAsTfExampleFile(args("output") + "/test", dataset)
    sc.close()
  }
}

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

  val n = 10000
  val in = 0.to(2 * n, 2).map(i => "%d,%d".format(i, i + 1))

  val tfRecordSpec =
    """{"version":1,""" +
      """"features":[{"name":"x_1","kind":"FloatList","tags":{}},""" +
      """{"name":"label","kind":"FloatList","tags":{}}],""" +
      """"compression":"UNCOMPRESSED"}"""

  "FeatureSpecJob" should "work" in {
    JobTest[FeatureSpecJob.type]
      .args("--input=in.txt", "--output=out")
      .input(TextIO("in.txt"), in)
      .output(TextIO("out/train/_tf_record_spec.json"))(_ should containSingleValue(tfRecordSpec))
      .output(TextIO("out/test/_tf_record_spec.json"))(_ should containSingleValue(tfRecordSpec))
      .output(TFExampleIO("out/train"))(_ should satisfy[Example](_.size === 9000+-500))
      .output(TFExampleIO("out/test"))(_ should satisfy[Example](_.size === 1000+-500))
      .run()
  }

  val tfRecordMSpec =
    """{"version":1,""" +
      """"features":[{"name":"x1","kind":"FloatList","tags":{"multispec-id":"0"}},""" +
      """{"name":"label","kind":"FloatList","tags":{"multispec-id":"1"}}],""" +
      """"compression":"UNCOMPRESSED"}"""

  "MultiSpecJob" should "work" in {
    JobTest[MultiSpecJob.type]
      .args("--input=in.txt", "--output=out")
      .input(TextIO("in.txt"), in)
      .output(TextIO("out/train/_tf_record_spec.json"))(_ should containSingleValue(tfRecordMSpec))
      .output(TextIO("out/test/_tf_record_spec.json"))(_ should containSingleValue(tfRecordMSpec))
      .output(TFExampleIO("out/train"))(_ should satisfy[Example](_.size === 9000+-500))
      .output(TFExampleIO("out/test"))(_ should satisfy[Example](_.size === 1000+-500))
      .run()
  }

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
