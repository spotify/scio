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

import com.spotify.zoltar.tf.{TensorFlowLoader, TensorFlowModel}
import com.spotify.zoltar.Model
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup, Teardown}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.types.TString
import org.tensorflow.ndarray.NdArrays
import org.tensorflow.proto.example.Example
import scala.jdk.CollectionConverters._

import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.zoltar.Model.Id
import java.util.concurrent.atomic.AtomicInteger

sealed trait PredictDoFn[T, V, M <: Model[_]]
    extends DoFnWithResource[T, V, PredictDoFn.Resource[M]] {
  import PredictDoFn._

  def modelId: String

  def loadModel: M

  def model: M = getResource.get(modelId)._2

  def withRunner(f: Session#Runner => V): V

  def extractInput(input: T): Map[String, Tensor]

  def extractOutput(input: T, out: Map[String, Tensor]): V

  def outputTensorNames: Seq[String]

  override def createResource(): Resource[M] = new ConcurrentHashMap[String, (AtomicInteger, M)]()

  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_CLASS

  @Setup
  override def setup(): Unit = {
    super.setup()
    val (a, _) = getResource.computeIfAbsent(
      modelId,
      new Function[String, (AtomicInteger, M)] {
        override def apply(v1: String): (AtomicInteger, M) =
          new AtomicInteger(0) -> loadModel
      }
    )
    a.incrementAndGet()
    ()
  }

  /** Process an element asynchronously. */
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
          Log.debug("Closing down output tensors")
          outTensors.asScala.foreach(_.close())
        }
      } finally {
        Log.debug("Closing down input tensors")
        i.foreach { case (_, t) => t.close() }
      }

      result
    }

    c.output(result)
  }

  @Teardown
  def teardown(): Unit = {
    Log.info(s"Tearing down predict DoFn $this")
    val (running, m) = getResource().get(modelId)
    if (running.decrementAndGet() == 0) {
      m.close()
    }
  }
}

object PredictDoFn {
  type Resource[M <: Model[_]] = ConcurrentMap[String, (AtomicInteger, M)]

  private val Log = LoggerFactory.getLogger(this.getClass)
}

abstract private[tensorflow] class SavedBundlePredictDoFn[T, V](
  uri: String,
  signatureName: String,
  options: TensorFlowModel.Options
) extends PredictDoFn[T, V, TensorFlowModel] {
  override def modelId: String =
    s"tf:$uri:$signatureName:${options.tags.asScala.mkString(":")}"

  override def loadModel: TensorFlowModel =
    TensorFlowLoader
      .create(Id.create(modelId), uri, options, signatureName)
      .get(Duration.ofDays(Integer.MAX_VALUE))

  override def withRunner(f: Session#Runner => V): V =
    f(model.instance().session().runner())
}

object SavedBundlePredictDoFn {
  def forRaw[T, V](
    uri: String,
    fetchOps: Seq[String],
    options: TensorFlowModel.Options,
    signatureName: String,
    inFn: T => Map[String, Tensor],
    outFn: (T, Map[String, Tensor]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
    override def extractInput(input: T): Map[String, Tensor] = inFn(input)

    override def extractOutput(input: T, out: Map[String, Tensor]): V = outFn(input, out)

    override def outputTensorNames: Seq[String] = fetchOps

    override def modelId: String = s"${super.modelId}:${fetchOps.mkString(":")}"
  }

  def forInput[T, V](
    uri: String,
    fetchOps: Option[Seq[String]],
    options: TensorFlowModel.Options,
    signatureName: String,
    inFn: T => Map[String, Tensor],
    outFn: (T, Map[String, Tensor]) => V
  ): SavedBundlePredictDoFn[T, V] = new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
    private lazy val exportedFetchOps =
      model.outputsNameMap().asScala.toMap
    private lazy val requestedFetchOps: Map[String, String] = fetchOps
      .map { tensorIds =>
        tensorIds.iterator.map(tensorId => tensorId -> exportedFetchOps(tensorId)).toMap
      }
      .getOrElse(exportedFetchOps)

    override def extractInput(input: T): Map[String, Tensor] = {
      val extractedInput = inFn(input)
      extractedInput.iterator.map { case (tensorId, tensor) =>
        model.inputsNameMap().get(tensorId) -> tensor
      }.toMap
    }

    override def extractOutput(input: T, out: Map[String, Tensor]): V =
      outFn(
        input,
        requestedFetchOps.iterator.map { case (tensorId, opName) =>
          tensorId -> out(opName)
        }.toMap
      )

    override def outputTensorNames: Seq[String] = requestedFetchOps.values.toSeq

    override def modelId: String = s"${super.modelId}:${fetchOps.toList.mkString(":")}"
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
    outFn: (T, Map[String, Tensor]) => V
  )(implicit ev: T <:< Example): SavedBundlePredictDoFn[T, V] =
    new SavedBundlePredictDoFn[T, V](uri, signatureName, options) {
      private lazy val exportedFetchOps =
        model.outputsNameMap().asScala.toMap
      private lazy val requestedFetchOps: Map[String, String] = fetchOps
        .map { tensorIds =>
          tensorIds.iterator.map(tensorId => tensorId -> exportedFetchOps(tensorId)).toMap
        }
        .getOrElse(exportedFetchOps)

      override def outputTensorNames: Seq[String] = requestedFetchOps.values.toSeq

      override def extractInput(input: T): Map[String, Tensor] = {
        val opName = model.inputsNameMap().get(exampleTensorName)
        val bytes = NdArrays.vectorOfObjects(input.toByteArray())
        val tensor = TString.tensorOfBytes(bytes)
        Map(opName -> tensor)
      }

      override def extractOutput(input: T, out: Map[String, Tensor]): V =
        outFn(
          input,
          requestedFetchOps.iterator.map { case (tensorId, opName) =>
            tensorId -> out(opName)
          }.toMap
        )

      override def modelId: String = s"${super.modelId}:${fetchOps.toList.mkString(":")}"
    }
}
