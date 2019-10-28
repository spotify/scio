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

package com.spotify.scio.tensorflow

import java.nio.FloatBuffer
import java.nio.charset.StandardCharsets
import java.util.Collections

import com.spotify.featran.FeatureSpec
import com.spotify.featran.tensorflow._
import com.spotify.featran.scio._
import com.spotify.featran.transformers.StandardScaler
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io._
import com.spotify.scio.testing._
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow._
import org.tensorflow.example.Example
import scala.io.Source

private[tensorflow] object TFSavedJob {

  case class Iris(
    sepalLength: Option[Double],
    sepalWidth: Option[Double],
    petalLength: Option[Double],
    petalWidth: Option[Double],
    className: Option[String]
  )

  val Spec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.petalLength)(StandardScaler("petal_length", withMean = true))
    .optional(_.petalWidth)(StandardScaler("petal_width", withMean = true))
    .optional(_.sepalLength)(StandardScaler("sepal_length", withMean = true))
    .optional(_.sepalWidth)(StandardScaler("sepal_width", withMean = true))

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines.mkString))

    val collection =
      sc.parallelize(List(Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa"))))

    Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predict(args("savedModelUri"), Seq("linear/head/predictions/class_ids"), options) { e =>
        Map("input_example_tensor" -> Tensors.create(Array(e.toByteArray)))
      } { (r, o) =>
        (r, o.map {
          case (a, outTensor) =>
            val output = Array.ofDim[Long](1)
            outTensor.copyTo(output)
            output(0)
        }.head)
      }
      .map(_._2)
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

private[tensorflow] object TFWithSigDefSavedJob {

  case class Iris(
    sepalLength: Option[Double],
    sepalWidth: Option[Double],
    petalLength: Option[Double],
    petalWidth: Option[Double],
    className: Option[String]
  )

  val Spec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.petalLength)(StandardScaler("petal_length", withMean = true))
    .optional(_.petalWidth)(StandardScaler("petal_width", withMean = true))
    .optional(_.sepalLength)(StandardScaler("sepal_length", withMean = true))
    .optional(_.sepalWidth)(StandardScaler("sepal_width", withMean = true))

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines.mkString))

    val collection =
      sc.parallelize(List(Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa"))))

    Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictWithSigDef(args("savedModelUri"), Seq("classes", "scores"), options) { e =>
        Map("inputs" -> Tensors.create(Array(e.toByteArray)))
      } { (_, o) =>
        val classes = asStringVector(o("classes").asInstanceOf[Tensor[String]])
        val scores = asFloatVector(o("scores").asInstanceOf[Tensor[Float]])
        classes zip scores
      }
      .flatMap { classesAndScores =>
        classesAndScores.map {
          case (clazz, score) =>
            "%s,%.3f".format(clazz, score)
        }
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }

  private def asFloatVector(tensor: Tensor[Float]): Vector[Float] = {
    val buffer = FloatBuffer.allocate(tensor.numElements)
    tensor.writeTo(buffer)
    Vector[Float](buffer.array: _*)
  }

  private def asStringVector(tensor: Tensor[String]): Vector[String] = {
    val buffer = Array.ofDim[Array[Byte]](1, tensor.numElements)
    tensor.copyTo(buffer)
    Vector(buffer(0).map(bytes => new String(bytes, StandardCharsets.UTF_8)): _*)
  }
}

class TensorflowSpec extends PipelineSpec {

  "predict" should "allow saved model prediction" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output")) { out =>
        out should containInAnyOrder(List("0"))
      }
      .run()
  }

  "predictWithSigDef" should "allow saved model prediction with default signature" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")
    val expected = List(
      "0,0.927",
      "1,0.066",
      "2,0.007"
    )

    JobTest[TFWithSigDefSavedJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output")) { out =>
        out should containInAnyOrder(expected)
      }
      .run()
  }
}
