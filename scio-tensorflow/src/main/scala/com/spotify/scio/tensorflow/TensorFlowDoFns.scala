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

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.function.Function

import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.zoltar.tf.TensorFlowModel
import com.spotify.zoltar.{Model, Models}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Teardown}
import org.tensorflow._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[this] abstract class PredictDoFn[T, V, M <: Model[_]](
  fetchOp: Seq[String],
  inFn: T => Map[String, Tensor[_]],
  outFn: (T, Map[String, Tensor[_]]) => V
) extends DoFnWithResource[T, V, ConcurrentMap[String, M]] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  def withRunner(f: Session#Runner => V): V

  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLASS

  /**
   * Process an element asynchronously.
   */
  @ProcessElement
  def processElement(c: DoFn[T, V]#ProcessContext): Unit = {
    val result = withRunner { runner =>
      val input = c.element()
      val i = inFn(input)
      var result: V = null.asInstanceOf[V]

      try {
        i.foreach { case (op, t) => runner.feed(op, t) }
        fetchOp.foreach(runner.fetch)
        val outTensors = runner.run()
        try {
          import scala.collection.breakOut
          result = outFn(input, (fetchOp zip outTensors.asScala)(breakOut))
        } finally {
          log.debug("Closing down output tensors")
          outTensors.asScala.foreach(_.close())
        }
      } finally {
        log.debug("Closing down input tensors")
        i.foreach { case (_, t) => t.close() }
      }

      result
    }

    c.output(result)
  }

  override def createResource(): ConcurrentMap[String, M] =
    new ConcurrentHashMap[String, M]()

}

private[tensorflow] class SavedBundlePredictDoFn[T, V](
  uri: String,
  options: TensorFlowModel.Options,
  fetchOp: Seq[String],
  inFn: T => Map[String, Tensor[_]],
  outFn: (T, Map[String, Tensor[_]]) => V
) extends PredictDoFn[T, V, TensorFlowModel](fetchOp, inFn, outFn) {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  override def withRunner(f: Session#Runner => V): V = {
    val model = getResource
      .computeIfAbsent(
        uri,
        new Function[String, TensorFlowModel] {
          override def apply(t: String): TensorFlowModel =
            Models
              .tensorFlow(uri, options)
              .get(Duration.ofDays(Integer.MAX_VALUE))
        }
      )

    f(model.instance().session().runner())
  }

  @Teardown
  def teardown(): Unit = {
    log.info(s"Tearing down predict DoFn $this")
    getResource.get(uri).close()
  }

}
