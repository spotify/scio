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

package com.spotify.scio.values

import com.spotify.scio.{Implicits, ScioContext}
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.transforms.{Combine, DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection, POutput}

import scala.reflect.ClassTag

private[values] trait PCollectionWrapper[T] extends TransformNameable {

  import Implicits._

  /** The [[org.apache.beam.sdk.values.PCollection PCollection]] being wrapped internally. */
  val internal: PCollection[T]

  /**
   * The [[ScioContext]] associated with this
   * [[org.apache.beam.sdk.values.PCollection PCollection]].
   */
  val context: ScioContext

  implicit val ct: ClassTag[T]

  private[scio] def applyInternal[Output <: POutput]
  (transform: PTransform[_ >: PCollection[T], Output]): Output =
    internal.apply(this.tfName, transform)

  protected def pApply[U: ClassTag]
  (transform: PTransform[_ >: PCollection[T], PCollection[U]]): SCollection[U] = {
    val t = if (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass) {
      // In case PCollection is windowed
      transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
    } else {
      transform
    }
    context.wrap(this.applyInternal(t))
  }

  private[scio] def parDo[U: ClassTag](fn: DoFn[T, U]): SCollection[U] =
    this.pApply(ParDo.of(fn)).setCoder(this.getCoder[U])

  private[scio] def getCoder[U: ClassTag]: Coder[U] =
    internal.getPipeline.getCoderRegistry.getScalaCoder[U](context.options)

  private[scio] def getKvCoder[K: ClassTag, V: ClassTag]: Coder[KV[K, V]] =
    internal.getPipeline.getCoderRegistry.getScalaKvCoder[K, V](context.options)

}
