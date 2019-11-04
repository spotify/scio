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

import com.spotify.zoltar.tf.{TensorFlowLoader, TensorFlowModel}
import com.spotify.zoltar.Model
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Teardown}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.example.Example
import scala.collection.JavaConverters._

import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.zoltar.Model.Id

sealed trait PredictDoFn[T, V, M <: Model[_]]
  extends DoFnWithResource[T, V, ConcurrentMap[String, M]] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  def withRunner(f: Session#Runner => V): V

  def extractInput(input: T): Map[String, Tensor[_]]

  def extractOutput(input: T, out: Map[String, Tensor[_]]): V

  def outputTensorNames: Seq[String]

  override def createResource(): ConcurrentMap[String, M] = new ConcurrentHashMap[String, M]()

  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_INSTANCE

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
          result = extractOutput(
            input,
            outputTensorNames.iterator.zip(outTensors.iterator().asScala).toMap
          )
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
  signatureName: String,
  options: TensorFlowModel.Options
) extends PredictDoFn[T, V, TensorFlowModel] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  @transient protected lazy val model: TensorFlowModel = getResource
    .computeIfAbsent(
      id,
      (_: String) => {
          TensorFlowLoader
            .create(Id.create(id), uri, options, signatureName)
            .get(Duration.ofDays(Integer.MAX_VALUE))
      }
    )

  override def withRunner(f: Session#Runner => V): V =
    f(model.instance().session().runner())

  def id(): String = {
    val tokens = Seq("tf", uri, signatureName) ++ Seq(options.tags.asScala.mkString(";"))
    tokens.mkString(":")
  }

  @Teardown
  def teardown(): Unit = {
    log.info(s"Tearing down predict DoFn $this")
    model.close()
  }
}

object SavedBundlePredictDoFn {
  def forRaw[T, V](
    uri: String,
    fetchOps: Seq[String],
    options: TensorFlowModel.Options,
    signatureName: String,
    inFn: T => Map[String, Tensor[_]],
    outFn: (T, Map[String, Tensor[_]]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
    override def extractInput(input: T): Map[String, Tensor[_]] = inFn(input)

    override def extractOutput(input: T, out: Map[String, Tensor[_]]): V = outFn(input, out)

    override def outputTensorNames: Seq[String] = fetchOps
  }

  def forInput[T, V](
    uri: String,
    fetchOps: Option[Seq[String]],
    options: TensorFlowModel.Options,
    signatureName: String,
    inFn: T => Map[String, Tensor[_]],
    outFn: (T, Map[String, Tensor[_]]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
    @transient private lazy val requestedFetchOps: Map[String, String] = {
      val exportedFetchOps = model.outputsNameMap().asScala.toMap
      fetchOps
        .map { tensorIds =>
          tensorIds.map { tensorId =>
            tensorId -> exportedFetchOps(tensorId)
          }.toMap
        }
        .getOrElse(exportedFetchOps)
    }

    override def extractInput(input: T): Map[String, Tensor[_]] = {
      val extractedInput = inFn(input)
      extractedInput.map {
        case (tensorId, tensor) =>
          model.inputsNameMap().get(tensorId) -> tensor
      }.toMap
    }

    override def extractOutput(input: T, out: Map[String, Tensor[_]]): V =
      outFn(input, requestedFetchOps.mapValues(out(_)))

    override def outputTensorNames: Seq[String] = requestedFetchOps.values.toSeq
  }

  /**
   * Note: if fetchOps isn't provided, then all outputs defined in the signature of the model
   * are retrieved. This can be expensive and unwanted, depending on the model.
   */
  def forTensorFlowExample[T, V](
    uri: String,
    exampleTensorName: String,
    fetchOps: Option[Seq[String]],
    options: TensorFlowModel.Options,
    signatureName: String,
    outFn: (T, Map[String, Tensor[_]]) => V
  )(implicit ev: T <:< Example): SavedBundlePredictDoFn[T, V] =
    new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
      @transient private lazy val requestedFetchOps: Map[String, String] = {
        val exportedFetchOps = model.outputsNameMap().asScala.toMap
        fetchOps
          .map { tensorIds =>
            tensorIds.map { tensorId =>
              tensorId -> exportedFetchOps(tensorId)
            }.toMap
          }
          .getOrElse(exportedFetchOps)
      }

      override def outputTensorNames: Seq[String] = requestedFetchOps.values.toSeq

      override def extractInput(input: T): Map[String, Tensor[_]] = {
        val i = model.inputsNameMap().get(exampleTensorName)
        Map(i -> Tensors.create(Array(input.toByteArray)))
      }

      override def extractOutput(input: T, out: Map[String, Tensor[_]]): V =
        outFn(input, requestedFetchOps.mapValues(out(_)))
    }
}
