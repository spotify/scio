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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.{DistCacheIO, PipelineSpec, TextIO}
import org.tensorflow._

private object TFJob {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(1L to 10)
      .predict(args("graphURI"), Seq("multiply"))
      {e => Map("input" -> Tensors.create(e))}
      {(r, o) => (r, o.map{case (_, t) => t.longValue()}.head)}
      .saveAsTextFile(args("output"))
    sc.close()
  }
}

private object TFJob2Inputs {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    sc.parallelize(1L to 10)
      .predict(args("graphURI"), Seq("multiply"))
      {e => Map("input" -> Tensors.create(e),
                "input2" -> Tensors.create(3L))}
      {(r, o) => (r, o.map{case (_, t) => t.longValue()}.head)}
      .saveAsTextFile(args("output"))
    sc.close()
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
      val graphFile = Files.createTempFile("tf-grap", ".bin")
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
    val t3 = Tensors.create(4L)
    try {
      val input = g.opBuilder("Placeholder", "input").setAttr("dtype", t3.dataType).build.output(0)
      val c3 = g.opBuilder("Const", "c3")
        .setAttr("dtype", t3.dataType)
        .setAttr("value", t3).build.output(0)
      g.opBuilder("Mul", "multiply").addInput(c3).addInput(input).build()
      JobTest[TFJob.type]
        .distCache(DistCacheIO[Array[Byte]]("tf-graph.bin"), g.toGraphDef)
        .args("--graphURI=tf-graph.bin", "--output=output")
        .output(TextIO("output")) {
          _ should containInAnyOrder((1L to 10).map(x => (x, x * 4)).map(_.toString))
        }
        .run()
    } finally {
      g.close()
      t3.close()
    }
  }

  it should "allow to predict with 2 inputs" in {
    val g = new Graph()
    try {
      val input = g.opBuilder("Placeholder", "input")
        .setAttr("dtype", DataType.INT64).build.output(0)
      val input2 = g.opBuilder("Placeholder", "input2")
        .setAttr("dtype", DataType.INT64).build.output(0)
      g.opBuilder("Mul", "multiply").addInput(input2).addInput(input).build()
      JobTest[TFJob2Inputs.type]
        .distCache(DistCacheIO[Array[Byte]]("tf-graph.bin"), g.toGraphDef)
        .args("--graphURI=tf-graph.bin", "--output=output")
        .output(TextIO("output")) {
          _ should containInAnyOrder((1L to 10).map(x => (x, x * 3)).map(_.toString))
        }
        .run()
    } finally {
      g.close()
    }
  }

}
