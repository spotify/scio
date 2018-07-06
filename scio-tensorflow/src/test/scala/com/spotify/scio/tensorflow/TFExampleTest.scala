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

import com.spotify.featran.{FeatureSpec, MultiFeatureSpec}
import com.spotify.featran.transformers.{OneHotEncoder, StandardScaler}
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.tensorflow.TFSavedJob.Iris
import com.spotify.scio.testing._
import org.tensorflow.example.Example
import org.tensorflow.metadata.v0.{FixedShape, Schema, SparseFeature}

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

object MultiSpecFeatranJob {
  import com.spotify.featran.scio._
  import com.spotify.featran.tensorflow._

  val fSpec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.petalLength)(StandardScaler("petal_length", withMean = true))
    .optional(_.petalWidth)(StandardScaler("petal_width", withMean = true))
    .optional(_.sepalLength)(StandardScaler("sepal_length", withMean = true))
    .optional(_.sepalWidth)(StandardScaler("sepal_width", withMean = true))

  val lSpec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.className)(OneHotEncoder("class_name"))

  val spec = MultiFeatureSpec(fSpec, lSpec)

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val collection =
      sc.parallelize(List(Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa"))))

    spec
      .extract(collection)
      .featureValues[Example]
      .saveAsTfExampleFile(args("output"))

    sc.close()
  }

}

class TFExampleTest extends PipelineSpec {

  "ExamplesJobV2" should "work" in {
    JobTest[ExamplesJobV2.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should haveSize(2))
      .run()
  }

  "ExamplesJobV2WithCustomSchema" should "work" in {
    JobTest[ExamplesJobV2WithSchema.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should haveSize(2))
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

  "MultiSpecFeatranJob" should "work" in {
    import scala.collection.JavaConverters._
    JobTest[MultiSpecFeatranJob.type]
      .args("--output=out")
      .output(TFExampleIO("out"))(_ should satisfy[Example]{i =>
        val features = i.head.getFeatures.getFeatureMap.asScala
        // check that there are features from both sides of the multispec, also that there is the
        // right number of features overall
        features.contains("petal_width") &&
          features.contains("class_name_Iris_setosa") &&
          features.size == 5
      })
      .run()
  }


}
