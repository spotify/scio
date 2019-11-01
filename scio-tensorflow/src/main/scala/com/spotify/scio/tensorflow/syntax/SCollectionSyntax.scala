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

import com.spotify.scio.coders.Coder
import com.spotify.scio.tensorflow.{
  SavedBundlePredictDoFn,
  TFExampleIO,
  TFRecordIO,
  TFSequenceExampleIO
}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import com.spotify.zoltar.tf.TensorFlowModel
import org.apache.beam.sdk.io.Compression
import org.tensorflow._
import org.tensorflow.example.{Example, SequenceExample}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with TensorFlow methods.
 */
final class PredictSCollectionOps[T: ClassTag](private val self: SCollection[T]) {
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
   */
  def predict[V: Coder, W](
    savedModelUri: String,
    fetchOps: Seq[String],
    options: TensorFlowModel.Options
  )(inFn: T => Map[String, Tensor[_]])(outFn: (T, Map[String, Tensor[_]]) => V): SCollection[V] =
    self.parDo(new SavedBundlePredictDoFn[T, V](savedModelUri, options, fetchOps, inFn, outFn))
}

final class ExampleSCollectionOps[T <: Example](private val self: SCollection[T]) extends AnyVal {
  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @return
   */
  @deprecated("saveAsTfExampleFile is deprecated: use saveAsTfRecordFile instead", "Scio 0.7.4")
  def saveAsTfExampleFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[Example] =
    saveAsTfRecordFile(path, suffix = suffix, compression = compression, numShards = numShards)

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[Example] = {
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[Example]].write(TFExampleIO(path))(param)
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
  @deprecated("saveAsTfExampleFile is deprecated: use saveAsTfRecordFile instead", "Scio 0.7.4")
  def saveAsTfExampleFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards
  ): ClosedTap[Example] =
    saveAsTfRecordFile(path, suffix = suffix, compression = compression, numShards = numShards)

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
   * `Array[Byte]`. The recommended record encoding is `org.tensorflow.example.Example` protocol
   * buffers (which contain `org.tensorflow.example.Features` as a field) serialized as bytes.
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
    self.asInstanceOf[SCollection[Array[Byte]]].write(TFRecordIO(path))(param)
  }
}

final class SequenceExampleSCollectionOps[T <: SequenceExample](private val self: SCollection[T])
    extends AnyVal {
  /**
   * Saves this SCollection of `org.tensorflow.example.SequenceExample` as a TensorFlow
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
    self.asInstanceOf[SCollection[SequenceExample]].write(TFSequenceExampleIO(path))(param)
  }
}

trait SCollectionSyntax {
  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[PredictSCollectionOps]].
   */
  implicit def tensorFlowPredictSCollectionOps[T: ClassTag](
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
