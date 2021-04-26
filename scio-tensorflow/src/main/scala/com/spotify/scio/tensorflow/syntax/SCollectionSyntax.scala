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

package com.spotify.scio.tensorflow.syntax

import org.apache.beam.sdk.io.Compression
import org.tensorflow._
import org.tensorflow.proto.example.{Example, SequenceExample}

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.tensorflow.{
  SavedBundlePredictDoFn,
  TFExampleIO,
  TFRecordIO,
  TFSequenceExampleIO
}
import com.spotify.scio.values.SCollection
import com.spotify.zoltar.tf.TensorFlowModel

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with TensorFlow methods. */
final class PredictSCollectionOps[T](private val self: SCollection[T]) {

  /**
   * Predict/infer/forward-pass on a TensorFlow Saved Model.
   *
   * @param savedModelUri URI of Saved TensorFlow model
   * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
   * @param options   configuration parameters for the session specified as a
   *                 `com.spotify.zoltar.tf.TensorFlowModel.Options`.
   * @param inFn     translates input elements of T to map of input-operation ->
   *                 [[org.tensorflow.Tensor Tensor]]. This method takes ownership of the
   *                 [[org.tensorflow.Tensor Tensor]]s.
   * @param outFn    translates output of prediction from map of output-operation ->
   *                 [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
   *                 ownership of the [[org.tensorflow.Tensor Tensor]]s.
   * @param signatureName  name of [[org.tensorflow.framework.SignatureDef]]s to be used
   *                       to run the prediction.
   */
  def predict[V: Coder, W](
    savedModelUri: String,
    fetchOps: Seq[String],
    options: TensorFlowModel.Options,
    signatureName: String = PredictSCollectionOps.DefaultSignatureName
  )(inFn: T => Map[String, Tensor])(outFn: (T, Map[String, Tensor]) => V): SCollection[V] =
    self.parDo(
      SavedBundlePredictDoFn
        .forRaw[T, V](savedModelUri, fetchOps, options, signatureName, inFn, outFn)
    )

  /**
   * Predict/infer/forward-pass on a TensorFlow Saved Model.
   * Only exported ops can be fetched.
   *
   * @param savedModelUri URI of Saved TensorFlow model
   * @param options   configuration parameters for the session specified as a
   *                 `com.spotify.zoltar.tf.TensorFlowModel.Options`.
   * @param fetchOps names of [[Option]] of [[org.tensorflow.Operation]]s to fetch the results from
   * @param inFn     translates input elements of T to map of input-operation ->
   *                 [[org.tensorflow.Tensor Tensor]]. This method takes ownership of the
   *                 [[org.tensorflow.Tensor Tensor]]s.
   * @param outFn    translates output of prediction from map of output-operation ->
   *                 [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
   *                 ownership of the [[org.tensorflow.Tensor Tensor]]s.
   * @param signatureName  name of [[org.tensorflow.framework.SignatureDef]]s to be used
   *                       to run the prediction.
   */
  def predictWithSigDef[V: Coder, W](
    savedModelUri: String,
    options: TensorFlowModel.Options,
    fetchOps: Option[Seq[String]] = PredictSCollectionOps.DefaultFetchOps,
    signatureName: String = PredictSCollectionOps.DefaultSignatureName
  )(inFn: T => Map[String, Tensor])(outFn: (T, Map[String, Tensor]) => V): SCollection[V] =
    self.parDo(
      SavedBundlePredictDoFn
        .forInput[T, V](savedModelUri, fetchOps, options, signatureName, inFn, outFn)
    )

  /**
   * Predict/infer/forward-pass on a TensorFlow Saved Model.
   * Only exported ops can be fetched.
   *
   * @param savedModelUri  URI of Saved TensorFlow model
   * @param options        configuration parameters for the session specified as a
   *                       `com.spotify.zoltar.tf.TensorFlowModel.Options`.
   * @param exampleInputOp name of [[org.tensorflow.Operation]]s to feed an example.
   * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
   * @param signatureName  name of [[org.tensorflow.framework.SignatureDef]]s to be used
   *                       to run the prediction.
   * @param outFn          translates output of prediction from map of output-operation ->
   *                       [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
   *                       ownership of the [[org.tensorflow.Tensor Tensor]]s.
   */
  def predictTfExamples[V: Coder](
    savedModelUri: String,
    options: TensorFlowModel.Options,
    exampleInputOp: String = PredictSCollectionOps.DefaultExampleInputOp,
    fetchOps: Option[Seq[String]] = PredictSCollectionOps.DefaultFetchOps,
    signatureName: String = PredictSCollectionOps.DefaultSignatureName
  )(outFn: (T, Map[String, Tensor]) => V)(implicit ev: T <:< Example): SCollection[V] =
    self.parDo(
      SavedBundlePredictDoFn.forTensorFlowExample[T, V](
        savedModelUri,
        exampleInputOp,
        fetchOps,
        options,
        signatureName,
        outFn
      )
    )
}

object PredictSCollectionOps {
  val DefaultSignatureName: String = "serving_default"
  val DefaultExampleInputOp: String = "inputs"
  val DefaultFetchOps: Option[Seq[String]] = None
}

final class ExampleSCollectionOps[T <: Example](private val self: SCollection[T]) extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.proto.example.Example` as a TensorFlow TFRecord file.
   *
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[Example] = {
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.covary[Example].write(TFExampleIO(path))(param)
  }
}

object SeqExampleSCollectionOps {
  private val mergeExamples: Seq[Example] => Example =
    _.foldLeft(Example.newBuilder)((b, i) => b.mergeFrom(i)).build()
}

final class SeqExampleSCollectionOps[T <: Example](private val self: SCollection[Seq[T]])
    extends AnyVal {
  def mergeExamples(e: Seq[Example]): Example = SeqExampleSCollectionOps.mergeExamples(e)

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[Example] =
    new ExampleSCollectionOps(self.map(SeqExampleSCollectionOps.mergeExamples))
      .saveAsTfRecordFile(path, suffix, compression, numShards)
}

final class TFRecordSCollectionOps[T <: Array[Byte]](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
   * `Array[Byte]`. The recommended record encoding is `org.tensorflow.proto.example.Example` protocol
   * buffers (which contain `org.tensorflow.proto.example.Features` as a field) serialized as bytes.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFRecordIO.WriteParam.DefaultSuffix,
    compression: Compression = TFRecordIO.WriteParam.DefaultCompression,
    numShards: Int = TFRecordIO.WriteParam.DefaultNumShards
  )(implicit ev: T <:< Array[Byte]): ClosedTap[Array[Byte]] = {
    val param = TFRecordIO.WriteParam(suffix, compression, numShards)
    self.covary[Array[Byte]].write(TFRecordIO(path))(param)
  }
}

final class SequenceExampleSCollectionOps[T <: SequenceExample](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.proto.example.SequenceExample` as a TensorFlow
   * TFRecord file.
   *
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[SequenceExample] = {
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.covary[SequenceExample].write(TFSequenceExampleIO(path))(param)
  }
}

trait SCollectionSyntax {

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[PredictSCollectionOps]].
   */
  implicit def tensorFlowPredictSCollectionOps[T](
    s: SCollection[T]
  ): PredictSCollectionOps[T] = new PredictSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFRecordSCollectionOps]].
   */
  implicit def tensorFlowTFRecordSCollectionOps[T <: Array[Byte]](
    s: SCollection[T]
  ): TFRecordSCollectionOps[T] = new TFRecordSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[ExampleSCollectionOps]].
   */
  implicit def tensorFlowExampleSCollectionOps[T <: Example](
    s: SCollection[T]
  ): ExampleSCollectionOps[T] = new ExampleSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[SeqExampleSCollectionOps]].
   */
  implicit def tensorFlowSeqExampleSCollectionOps[T <: Example](
    s: SCollection[Seq[T]]
  ): SeqExampleSCollectionOps[T] = new SeqExampleSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[SequenceExampleSCollectionOps]].
   */
  implicit def tensorFlowSequenceExampleSCollectionOps[T <: SequenceExample](
    s: SCollection[T]
  ): SequenceExampleSCollectionOps[T] = new SequenceExampleSCollectionOps(s)
}
