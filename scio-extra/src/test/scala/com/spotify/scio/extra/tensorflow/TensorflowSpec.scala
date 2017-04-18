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

package com.spotify.scio.extra.tensorflow

import org.scalatest.{FlatSpec, Matchers}
import org.tensorflow.{Graph, Session, Tensor, TensorFlow}

class TensorflowSpec extends FlatSpec with Matchers {

  "Tensorflow" should "work for hello-wold" in {
    val const = "MyConst"
    val graph = new Graph()
    val helloworld = s"Hello from ${TensorFlow.version()}"
    val t = Tensor.create(helloworld.getBytes("UTF-8"))
    graph.opBuilder("Const", const).setAttr("dtype", t.dataType()).setAttr("value", t).build()
    val session = new Session(graph)
    val r = session.runner().fetch(const).run().get(0)
    new String(r.bytesValue()) should be (helloworld)
  }

}
