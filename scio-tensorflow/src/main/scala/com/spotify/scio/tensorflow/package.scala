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

package com.spotify.scio

import com.spotify.scio.testing.TestIO
import com.spotify.scio.values._
import org.tensorflow.example.Example

import scala.reflect.ClassTag

package object tensorflow {

  import scala.language.implicitConversions

  case class TFRecordIO(path: String) extends TestIO[Array[Byte]](path)

  case class TFExampleIO(path: String) extends TestIO[Example](path)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TensorFlowSCollectionFunctions]].
   */
  implicit def makeTensorFlowSCollectionFunctions[T: ClassTag](s: SCollection[T])
  : TensorFlowSCollectionFunctions[T] = new TensorFlowSCollectionFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFRecordSCollectionFunctions]].
   */
  implicit def makeTFRecordSCollectionFunctions[T <: Array[Byte]](s: SCollection[T])
  : TFRecordSCollectionFunctions[T] = new TFRecordSCollectionFunctions(s)

  /** Implicit conversion from [[ScioContext]] to [[TFRecordSCollectionFunctions]]. */
  implicit def makeTFScioContextFunctions(s: ScioContext): TFScioConextFunctions =
    new TFScioConextFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFExampleSCollectionFunctions]].
   */
  implicit def makeTFExampleSCollectionFunctions[T <: Example](s: SCollection[T])
  : TFExampleSCollectionFunctions[T] = new TFExampleSCollectionFunctions(s)
}
