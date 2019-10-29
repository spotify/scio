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

import com.spotify.zoltar.tf.{TensorFlowLoader, TensorFlowModel}
import com.spotify.zoltar.{Model, Models}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup, Teardown}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.example.Example
import scala.collection.JavaConverters._

import com.spotify.zoltar.Model.Id

private[this] abstract class PredictDoFn[T, V, M <: Model[_]] extends DoFn[T, V] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)
  protected var model: M = _

  def withRunner(f: Session#Runner => V): V

  def extractInput(input: T): Map[String, Tensor[_]]

  def extractOutput(input: T, out: Map[String, Tensor[_]]): V

  def outputTensorNames: Seq[String]

  /**
   * Process an element asynchronously.
   */
  @ProcessElement
  def processElement(c: DoFn[T, V]#ProcessContext): Unit = {
    val result = withRunner { runner =>
      val input = c.element()
      val i = extractInput(input)
      var result: V = null.asInstanceOf[V]

      try {
        i.foreach { case (op, t) => runner.feed(op, t) }
        outputTensorNames.foreach(runner.fetch)
        val outTensors = runner.run()
        try {
          import scala.collection.breakOut
          result = extractOutput(input, (outputTensorNames zip outTensors.asScala)(breakOut))
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
}

private[tensorflow] abstract class SavedBundlePredictDoFn[T, V](
  uri: String,
  options: TensorFlowModel.Options
) extends PredictDoFn[T, V, TensorFlowModel] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  @Setup
  def setup(): Unit =
    model = Models
      .tensorFlow(uri, options)
      .get(Duration.ofDays(Integer.MAX_VALUE))

  override def withRunner(f: Session#Runner => V): V = f(model.instance().session().runner())

  @Teardown
  def teardown(): Unit = {
    log.info(s"Tearing down predict DoFn $this")
    model.close()
    model = null
  }
}

object SavedBundlePredictDoFn {
  def forInput[T, V](
    uri: String,
    options: TensorFlowModel.Options,
    fetchOp: Seq[String],
    inFn: T => Map[String, Tensor[_]],
    outFn: (T, Map[String, Tensor[_]]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, options) {
    override def extractInput(input: T): Map[String, Tensor[_]] = inFn(input)

    override def extractOutput(input: T, out: Map[String, Tensor[_]]): V = outFn(input, out)

    override def outputTensorNames: Seq[String] = fetchOp
  }

  def forTensorFlowExample[T <: Example, V](
    uri: String,
    exampleTensorName: String,
    signatureName: String,
    options: TensorFlowModel.Options,
    outFn: (T, Map[String, Tensor[_]]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, options) {

    var fetchOps: Seq[String] = _

    override def setup(): Unit = {
      model = TensorFlowLoader
        .create(Id.create("tensorflow"), uri, options, signatureName)
        .get(Duration.ofDays(Integer.MAX_VALUE))
      fetchOps = model.outputsNameMap().values().asScala.toList
    }

    override def outputTensorNames: Seq[String] = fetchOps

    override def extractInput(input: T): Map[String, Tensor[_]] = {
      val i = model.inputsNameMap().get(exampleTensorName)
      Map(i -> Tensors.create(input.toByteArray))
    }

    override def extractOutput(input: T, out: Map[String, Tensor[_]]): V =
      outFn(input, out)
  }
}
