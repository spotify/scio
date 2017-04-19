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

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import org.tensorflow._

class TensorflowSpec extends PipelineSpec {

  private def createHelloWorldGraph = {
    val const = "MyConst"
    val graph = new Graph()
    val helloworld = s"Hello from ${TensorFlow.version()}"
    val t = Tensor.create(helloworld.getBytes("UTF-8"))
    graph.opBuilder("Const", const).setAttr("dtype", t.dataType()).setAttr("value", t).build()
    (graph, const, helloworld)
  }

  "Tensorflow" should "work for hello-wold" in {
    val (graph, const, helloworld) = createHelloWorldGraph
    val session = new Session(graph)
    val r = session.runner().fetch(const).run().get(0)
    new String(r.bytesValue()) should be (helloworld)
  }

  it should "work for serde model" in {
    val (graph, const, helloworld) = createHelloWorldGraph
    val graphFile = Files.createTempFile("tf-grap", ".bin")
    Files.write(graphFile, graph.toGraphDef)
    val newGraph = new Graph()
    newGraph.importGraphDef(Files.readAllBytes(graphFile))
    val session = new Session(graph)
    val r = session.runner().fetch(const).run().get(0)
    new String(r.bytesValue()) should be (helloworld)
  }

  it should "allow to predict" in {
    val g = new Graph()
    val t3 = Tensor.create(3L)
    val input = g.opBuilder("Placeholder", "input").setAttr("dtype", t3.dataType).build.output(0)
    val c3 = g.opBuilder("Const", "c3")
      .setAttr("dtype", t3.dataType)
      .setAttr("value", t3).build.output(0)
    g.opBuilder("Mul", "multiply").addInput(c3).addInput(input).build()
    val graphFile = Files.createTempFile("tf-grap", ".bin")
    Files.write(graphFile, g.toGraphDef)
    runWithContext { sc =>
      sc.parallelize(1L to 10).map(Tensor.create)
        .predict(graphFile.toString, "input", Seq("multiply"))
        .flatMap(e => e.map(_.longValue())) should containInAnyOrder ((1L to 10).map(_ * 3))
    }
  }

}
