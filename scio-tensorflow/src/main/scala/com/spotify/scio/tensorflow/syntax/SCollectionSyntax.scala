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
import org.tensorflow.proto.{Example, SequenceExample}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.tensorflow.{TFExampleIO, TFRecordIO, TFSequenceExampleIO}
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection

final class ExampleSCollectionOps[T <: Example](private val self: SCollection[T]) extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.proto.Example` as a TensorFlow TFRecord file.
   *
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards,
    shardNameTemplate: String = TFExampleIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = TFExampleIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      TFExampleIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = TFExampleIO.WriteParam.DefaultPrefix
  ): ClosedTap[Example] = {
    val param = TFExampleIO.WriteParam(
      suffix,
      compression,
      numShards,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
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
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files. Caveat: if some
   * feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards,
    shardNameTemplate: String = TFExampleIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = TFExampleIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      TFExampleIO.WriteParam.DefaultFilenamePolicySupplier
  ): ClosedTap[Example] =
    new ExampleSCollectionOps(self.map(SeqExampleSCollectionOps.mergeExamples))
      .saveAsTfRecordFile(
        path,
        suffix,
        compression,
        numShards,
        shardNameTemplate,
        tempDirectory,
        filenamePolicySupplier
      )
}

final class TFRecordSCollectionOps[T <: Array[Byte]](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
   * `Array[Byte]`. The recommended record encoding is `org.tensorflow.proto.Example` protocol
   * buffers (which contain `org.tensorflow.proto.Features` as a field) serialized as bytes.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFRecordIO.WriteParam.DefaultSuffix,
    compression: Compression = TFRecordIO.WriteParam.DefaultCompression,
    numShards: Int = TFRecordIO.WriteParam.DefaultNumShards,
    shardNameTemplate: String = TFExampleIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = TFExampleIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      TFExampleIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = TFExampleIO.WriteParam.DefaultPrefix
  )(implicit ev: T <:< Array[Byte]): ClosedTap[Array[Byte]] = {
    val param = TFRecordIO.WriteParam(
      suffix,
      compression,
      numShards,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.covary[Array[Byte]].write(TFRecordIO(path))(param)
  }
}

final class SequenceExampleSCollectionOps[T <: SequenceExample](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.proto.SequenceExample` as a TensorFlow TFRecord file.
   *
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards,
    shardNameTemplate: String = TFExampleIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = TFExampleIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      TFExampleIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = TFExampleIO.WriteParam.DefaultPrefix
  ): ClosedTap[SequenceExample] = {
    val param = TFExampleIO.WriteParam(
      suffix,
      compression,
      numShards,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.covary[SequenceExample].write(TFSequenceExampleIO(path))(param)
  }
}

trait SCollectionSyntax {

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
