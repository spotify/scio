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

package com.spotify.scio.extra

import com.spotify.scio.extra.sparkey.instances._

/**
 * Main package for Sparkey side input APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.extra.sparkey._
 * }}}
 *
 * To save an `SCollection[(String, String)]` to a Sparkey fileset:
 * {{{
 * val s = sc.parallelize(Seq("a" -> "one", "b" -> "two"))
 *
 * // temporary location
 * val s1: SCollection[SparkeyUri] = s.asSparkey
 *
 * // specific location
 * val s1: SCollection[SparkeyUri] = s.asSparkey("gs://<bucket>/<path>/<sparkey-prefix>")
 * }}}
 *
 * // with multiple shards, sharded by MurmurHash3 of the key val s1: SCollection[SparkeyUri] =
 * s.asSparkey("gs://<bucket>/<path>/<sparkey-dir>", numShards=2) }}}
 *
 * The result `SCollection[SparkeyUri]` can be converted to a side input:
 * {{{
 * val s: SCollection[SparkeyUri] = sc.parallelize(Seq("a" -> "one", "b" -> "two")).asSparkey
 * val side: SideInput[SparkeyReader] = s.asSparkeySideInput
 * }}}
 *
 * These two steps can be done with a syntactic sugar:
 * {{{
 * val side: SideInput[SparkeyReader] = sc
 *   .parallelize(Seq("a" -> "one", "b" -> "two"))
 *   .asSparkeySideInput
 * }}}
 *
 * An existing Sparkey file can also be converted to a side input directly:
 * {{{
 * sc.sparkeySideInput("gs://<bucket>/<path>/<sparkey-prefix>")
 * }}}
 *
 * A sharded collection of Sparkey files can also be used as a side input by specifying a glob path:
 * {{{
 * sc.sparkeySideInput("gs://<bucket>/<path>/<sparkey-dir>/part-*")
 * }}}
 *
 * `SparkeyReader` can be used like a lookup table in a side input operation:
 * {{{
 * val main: SCollection[String] = sc.parallelize(Seq("a", "b", "c"))
 * val side: SideInput[SparkeyReader] = sc
 *   .parallelize(Seq("a" -> "one", "b" -> "two"))
 *   .asSparkeySideInput
 *
 * main.withSideInputs(side)
 *   .map { (x, s) =>
 *     s(side).getOrElse(x, "unknown")
 *   }
 * }}}
 *
 * A `SparkeyMap` can store any types of keys and values, but can only be used as a SideInput:
 * {{{
 * val main: SCollection[String] = sc.parallelize(Seq("a", "b", "c"))
 * val side: SideInput[SparkeyMap[String, Int]] = sc
 *   .parallelize(Seq("a" -> 1, "b" -> 2, "c" -> 3))
 *   .asLargeMapSideInput()
 *
 * val objects: SCollection[MyObject] = main
 *   .withSideInputs(side)
 *   .map { (x, s) => s(side).get(x) }
 *   .toSCollection
 * }}}
 *
 * To read a static Sparkey collection and use it as a typed SideInput, use `TypedSparkeyReader`.
 * `TypedSparkeyReader` can also accept a Caffeine cache to reduce IO and deserialization load:
 *
 * {{{
 * val main: SCollection[String] = sc.parallelize(Seq("a", "b", "c"))
 * val cache: Cache[String, MyObject] = ...
 * val side: SideInput[TypedSparkeyReader[MyObject]] = sc
 *   .typedSparkeySideInput("gs://<bucket>/<path>/<sparkey-prefix>", MyObject.decode, cache)
 *
 * val objects: SCollection[MyObject] = main
 *   .withSideInputs(side)
 *   .map { (x, s) => s(side).get(x) }
 *   .toSCollection
 * }}}
 */
package object sparkey extends syntax.AllSyntax with SparkeyReaderInstances with WritableInstances {

  @deprecated("Use syntax.SparkeyScioContextOps instead", "0.14.0")
  type SparkeyScioContext = syntax.SparkeyScioContextOps
  @deprecated("Use syntax.SparkeySCollectionOps instead", "0.14.0")
  type SparkeyPairSCollection[K, V] = syntax.SparkeyPairSCollectionOps[K, V]
  @deprecated("Use syntax.SparkeySetSCollectionOps instead", "0.14.0")
  type SparkeySetSCollection[T] = syntax.SparkeySetSCollectionOps[T]
  @deprecated("Use syntax.SparkeySCollectionOps instead", "0.14.0")
  type SparkeySCollection = syntax.SparkeySCollectionOps

}
