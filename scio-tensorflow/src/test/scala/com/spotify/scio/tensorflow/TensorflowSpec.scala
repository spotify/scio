/*
 * Copyright 2017 Spotify AB.
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
import java.util.Collections

import com.spotify.featran.scio._
import com.spotify.featran.tensorflow._
import com.spotify.featran.FeatureSpec
import com.spotify.featran.transformers.StandardScaler
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.{PipelineSpec, TextIO}
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow._
import org.tensorflow.example.Example

import scala.io.Source

private object TFGraphJob {

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(1L to 10)
      .predict(args("graphURI"), Seq("multiply")) { e =>
        Map("input" -> Tensors.create(e))
      } { (r, o) =>
        (r, o.map { case (_, t) => t.longValue() }.head)
      }
      .saveAsTextFile(args("output"))
    sc.close().waitUntilDone()
  }
}

private object TFGraphJob2Inputs {

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(1L to 10)
      .predict(args("graphURI"), Seq("multiply")) { e =>
        Map("input" -> Tensors.create(e), "input2" -> Tensors.create(3L))
      } { (r, o) =>
        (r, o.map { case (_, t) => t.longValue() }.head)
      }
      .saveAsTextFile(args("output"))
    sc.close().waitUntilDone()
  }
}

private object TFSavedJob {

  case class Iris(sepalLength: Option[Double],
                  sepalWidth: Option[Double],
                  petalLength: Option[Double],
                  petalWidth: Option[Double],
                  className: Option[String])

  val Spec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.petalLength)(StandardScaler("petal_length", withMean = true))
    .optional(_.petalWidth)(StandardScaler("petal_width", withMean = true))
    .optional(_.sepalLength)(StandardScaler("sepal_length", withMean = true))
    .optional(_.sepalWidth)(StandardScaler("sepal_width", withMean = true))

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder.tags(Collections.singletonList("serve")).build
    val settings = sc.parallelize(List(Source.fromURL(args("settings")).getLines.mkString))

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

    sc.close().waitUntilDone()
  }
}

class TensorflowSpec extends PipelineSpec {

  private def createHelloWorldGraph = {
    val const = "MyConst"
    val graph = new Graph()
    val helloworld = s"Hello from ${TensorFlow.version()}"
    val t = Tensors.create(helloworld)
    graph.opBuilder("Const", const).setAttr("dtype", t.dataType()).setAttr("value", t).build()
    t.close()
    (graph, const, helloworld)
  }

  "Tensorflow" should "work for hello-wold" in {
    val (graph, const, helloworld) = createHelloWorldGraph
    val session = new Session(graph)
    val r = session.runner().fetch(const).run().get(0)
    try {
      new String(r.bytesValue()) should be(helloworld)
    } finally {
      session.close()
      graph.close()
      r.close()
    }
  }

  it should "work for serde model" in {
    val (graph, const, helloworld) = createHelloWorldGraph
    val session = new Session(graph)
    val r = session.runner().fetch(const).run().get(0)
    val newGraph = new Graph()
    try {
      val graphFile = Files.createTempFile("tf-graph", ".bin")
      Files.write(graphFile, graph.toGraphDef)
      newGraph.importGraphDef(Files.readAllBytes(graphFile))
      new String(r.bytesValue()) should be(helloworld)
    } finally {
      session.close()
      graph.close()
      newGraph.close()
      r.close()
    }
  }

  it should "allow to predict" in {
    val g = new Graph()
    val t3 = Tensors.create(3L)
    val graphFile = Files.createTempFile("tf-graph", ".bin")
    try {
      val input = g.opBuilder("Placeholder", "input").setAttr("dtype", t3.dataType).build.output(0)
      val c3 = g
        .opBuilder("Const", "c3")
        .setAttr("dtype", t3.dataType)
        .setAttr("value", t3)
        .build
        .output(0)
      g.opBuilder("Mul", "multiply").addInput(c3).addInput(input).build()

      Files.write(graphFile, g.toGraphDef)

      JobTest[TFGraphJob.type]
        .args(s"--graphURI=${graphFile.toUri}", "--output=output")
        .output(TextIO("output")) {
          _ should containInAnyOrder((1L to 10).map(x => (x, x * 3)).map(_.toString))
        }
        .run()
    } finally {
      g.close()
      t3.close()
      graphFile.toFile.deleteOnExit()
    }
  }

  it should "allow to predict with 2 inputs" in {
    val g = new Graph()
    val graphFile = Files.createTempFile("tf-graph", ".bin")
    try {
      val input = g
        .opBuilder("Placeholder", "input")
        .setAttr("dtype", DataType.INT64)
        .build
        .output(0)
      val input2 = g
        .opBuilder("Placeholder", "input2")
        .setAttr("dtype", DataType.INT64)
        .build
        .output(0)
      g.opBuilder("Mul", "multiply").addInput(input2).addInput(input).build()

      Files.write(graphFile, g.toGraphDef)

      JobTest[TFGraphJob2Inputs.type]
        .args(s"--graphURI=${graphFile.toUri}", "--output=output")
        .output(TextIO("output")) {
          _ should containInAnyOrder((1L to 10).map(x => (x, x * 3)).map(_.toString))
        }
        .run()
    } finally {
      g.close()
      graphFile.toFile.deleteOnExit()
    }
  }

  it should "allow saved model prediction" in {
    val resource = getClass.getResource("/trained_model")
    val settings = getClass.getResource("/settings.json")

    JobTest[TFSavedJob.type]
      .args(s"--savedModelUri=$resource", s"--settings=$settings", "--output=output")
      .output(TextIO("output")) {
        _ should containInAnyOrder(List("0"))
      }
      .run()
  }

}
