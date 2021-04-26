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

import java.util.Collections

import com.spotify.featran.FeatureSpec
import com.spotify.featran.tensorflow._
import com.spotify.featran.scio._
import com.spotify.featran.transformers.StandardScaler
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io._
import com.spotify.scio.testing._
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow.types.TString
import org.tensorflow.proto.example.Example

import scala.io.Source
import org.tensorflow.ndarray.NdArrays
import org.tensorflow.types.TFloat32
import org.tensorflow.types.TInt64

import scala.jdk.CollectionConverters._

private[tensorflow] object TFSavedSpec {
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
}

object TFSavedRawJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predict(args("savedModelUri"), Seq("linear/head/predictions/class_ids"), options) { e =>
        val tensor = TString.tensorOfBytes(NdArrays.vectorOfObjects(e.toByteArray()))
        Map("input_example_tensor" -> tensor)
      } { (_, o) =>
        val tensor = o("linear/head/predictions/class_ids")
        tensor.asInstanceOf[TInt64].getLong()
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedTensorsMapInputDefaultSigDefJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictWithSigDef(args("savedModelUri"), options) { e =>
        val tensor = TString.tensorOfBytes(NdArrays.vectorOfObjects(e.toByteArray()))
        Map("inputs" -> tensor)
      } { (_, o) =>
        val classes = o("classes").asInstanceOf[TString].get(0)
        val scores = o("scores").asInstanceOf[TFloat32].get(0)
        classes
          .scalars()
          .iterator()
          .asScala
          .zip(scores.scalars().iterator().asScala)
          .map(t => (t._1.getObject(), t._2.getObject()))
          .maxBy(_._2)
          ._1
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedTensorsMapInputPredictSigDefJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictWithSigDef(args("savedModelUri"), options, signatureName = "predict") { e =>
        val tensor = TString.tensorOfBytes(NdArrays.vectorOfObjects(e.toByteArray()))
        Map("examples" -> tensor)
      } { (_, o) =>
        // get the highest probability class
        o("classes").asInstanceOf[TString].get(0).getObject()
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedTensorsMapInputPredictSigDefSpecifiedFetchOpsJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictWithSigDef(
        args("savedModelUri"),
        options,
        fetchOps = Some(Seq("classes")),
        signatureName = "predict"
      )(e => Map("examples" -> TString.tensorOfBytes(NdArrays.vectorOfObjects(e.toByteArray())))) {
        (_, o) =>
          // get the highest probability class
          o("classes").asInstanceOf[TString].get(0).getObject()
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedExampleInputDefaultSigDefJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictTfExamples(
        savedModelUri = args("savedModelUri"),
        options = options
      ) { (_, o) =>
        val classes = o("classes").asInstanceOf[TString].get(0)
        val scores = o("scores").asInstanceOf[TFloat32].get(0)
        // get the highest probability class
        classes
          .scalars()
          .iterator()
          .asScala
          .zip(scores.scalars().iterator().asScala)
          .map(t => (t._1.getObject(), t._2.getObject()))
          .maxBy(_._2)
          ._1
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedExampleInputPredictSigDefJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictTfExamples(
        savedModelUri = args("savedModelUri"),
        options = options,
        exampleInputOp = "examples",
        signatureName = "predict"
      ) { (_, o) =>
        // get the highest probability class
        o("classes").asInstanceOf[TString].get(0).getObject()
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

object TFSavedExampleInputPredictSigDefSpecifiedFetchOpsJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder
      .tags(Collections.singletonList("serve"))
      .build
    val settings =
      sc.parallelize(List(Source.fromURL(args("settings")).getLines().mkString))

    val collection =
      sc.parallelize(
        List(TFSavedSpec.Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
      )

    TFSavedSpec.Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predictTfExamples(
        savedModelUri = args("savedModelUri"),
        options = options,
        exampleInputOp = "examples",
        fetchOps = Some(Seq("classes")),
        signatureName = "predict"
      ) { (_, o) =>
        // get the highest probability class
        o("classes").asInstanceOf[TString].get(0).getObject()
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
    ()
  }
}

class TensorflowSpec extends PipelineSpec {
  it should "allow saved model prediction with raw inputs and outputs" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedRawJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with feature tensors" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedTensorsMapInputDefaultSigDefJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with feature tensors and predict sig-def" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedTensorsMapInputPredictSigDefJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with feature tensors, predict sig-def, and specified fetch ops" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedTensorsMapInputPredictSigDefSpecifiedFetchOpsJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with tf example with default sig-def" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedExampleInputDefaultSigDefJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with tf example with predict sig-def" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedExampleInputPredictSigDefJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }

  it should "allow saved model prediction with tf example with predict sig-def and fetchOps specified" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedExampleInputPredictSigDefSpecifiedFetchOpsJob.type]
      .args(
        s"--savedModelUri=$resource",
        s"--settings=$settings",
        "--output=output"
      )
      .output(TextIO("output"))(out => out should containInAnyOrder(List("0")))
      .run()
  }
}
