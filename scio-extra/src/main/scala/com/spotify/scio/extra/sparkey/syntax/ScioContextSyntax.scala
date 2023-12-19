/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.extra.sparkey.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.sparkey.{SparkeySideInput, SparkeyUri}
import com.spotify.scio.extra.sparkey.instances.{CachedStringSparkeyReader, TypedSparkeyReader}
import com.spotify.scio.util.Cache
import com.spotify.scio.values.SideInput
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.PCollectionView

/** Enhanced version of [[ScioContext]] with Sparkey methods. */
class SparkeyScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Create a SideInput of `SparkeyReader` from a [[SparkeyUri]] base path, to be used with
   * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. If the
   * provided base path ends with "*", it will be treated as a sharded collection of Sparkey files.
   */
  @experimental
  def sparkeySideInput(basePath: String): SideInput[SparkeyReader] = {
    val paths = Seq[SparkeyUri](SparkeyUri(basePath))
    val view: PCollectionView[SparkeyUri] = self
      .parallelize(paths)
      .applyInternal(View.asSingleton())
    new SparkeySideInput(view)
  }

  /**
   * Create a SideInput of `TypedSparkeyReader` from a [[SparkeyUri]] base path, to be used with
   * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. The provided
   * decoder function will map from the underlying byte array to a JVM type, and the optional
   * [[Cache]] object can be used to cache reads in memory after decoding.
   */
  @experimental
  def typedSparkeySideInput[T](
    basePath: String,
    decoder: Array[Byte] => T,
    cache: Cache[String, T] = null
  ): SideInput[TypedSparkeyReader[T]] =
    sparkeySideInput(basePath).map(reader => new TypedSparkeyReader[T](reader, decoder, cache))

  /**
   * Create a SideInput of `CachedStringSparkeyReader` from a [[SparkeyUri]] base path, to be used
   * with [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
   */
  @experimental
  def cachedStringSparkeySideInput[T](
    basePath: String,
    cache: Cache[String, String]
  ): SideInput[CachedStringSparkeyReader] =
    sparkeySideInput(basePath).map(reader => new CachedStringSparkeyReader(reader, cache))
}

trait ScioContextSyntax {
  implicit def sparkeyScioContextOps(sc: ScioContext): SparkeyScioContextOps =
    new SparkeyScioContextOps(sc)
}
