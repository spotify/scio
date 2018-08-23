/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.tensorflow.example.Example

import scala.reflect.ClassTag
import scala.language.implicitConversions

trait TensorFlowImplicits {

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[PredictSCollectionFunctions]].
   */
  implicit def makePredictSCollectionFunctions[T: ClassTag](
    s: SCollection[T]): PredictSCollectionFunctions[T] = new PredictSCollectionFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFRecordSCollectionFunctions]].
   */
  implicit def makeTFRecordSCollectionFunctions[T <: Array[Byte]](
    s: SCollection[T]): TFRecordSCollectionFunctions[T] = new TFRecordSCollectionFunctions(s)

  /** Implicit conversion from [[ScioContext]] to [[TFScioContextFunctions]]. */
  implicit def makeTFScioContextFunctions(s: ScioContext): TFScioContextFunctions =
    new TFScioContextFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFExampleSCollectionFunctions]].
   */
  implicit def makeTFExampleSCollectionFunctions[T <: Example](
    s: SCollection[T]): TFExampleSCollectionFunctions[T] = new TFExampleSCollectionFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[SeqTFExampleSCollectionFunctions]].
   */
  implicit def makeSeqTFExampleSCollectionFunctions[T <: Example](
    s: SCollection[Seq[T]]): SeqTFExampleSCollectionFunctions[T] =
    new SeqTFExampleSCollectionFunctions(s)

}

/**
 * This allows to not only use [[TensorFlowImplicits]] as:
 * {{{
 * import TensorFlowImplicits._
 * }}}
 *
 * But also make them available from other places through inheritance, see
 * [[com.spotify.scio.tensorflow tensorflow]].
 */
object TensorFlowImplicits extends TensorFlowImplicits
