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

import java.lang.Math.floorMod
import java.util.{UUID, Iterator => JIterator}

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.sparkey.instances.{
  CachedStringSparkeyReader,
  SparkeyReaderInstances,
  TypedSparkeyReader
}
import com.spotify.scio.util.Cache
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.{IndexHeader, LogHeader, SparkeyReader}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.hashing.MurmurHash3

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
 * // with multiple shards, sharded by MurmurHash3 of the key
 * val s1: SCollection[SparkeyUri] = s.asSparkey("gs://<bucket>/<path>/<sparkey-dir>", numShards=2)
 * }}}
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
 * A `TypedSparkeyReader` can be used to do automatic decoding of JVM types from byte values:
 * {{{
 * val main: SCollection[String] = sc.parallelize(Seq("a", "b", "c"))
 * val side: SideInput[TypedSparkeyReader[MyObject]] = sc
 *   .typedSparkeySideInput("gs://<bucket>/<path>/<sparkey-prefix>", MyObject.decode)
 *
 * val objects: SCollection[MyObject] = main
 *   .withSideInputs(side)
 *   .map { (x, s) => s(side).get(x) }
 *   .toSCollection
 * }}}
 *
 * A `TypedSparkeyReader` can also accept a Caffeine cache to reduce IO and deserialization load:
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
package object sparkey extends SparkeyReaderInstances {

  /** Enhanced version of [[ScioContext]] with Sparkey methods. */
  implicit class SparkeyScioContext(private val self: ScioContext) extends AnyVal {
    private def singleViewOf(basePath: String): PCollectionView[SparkeyUri] =
      self.parallelize(Seq(SparkeyUri(basePath, self.options))).applyInternal(View.asSingleton())

    private def shardedViewOf(basePath: String): PCollectionView[SparkeyUri] =
      self
        .parallelize(Seq[SparkeyUri](ShardedSparkeyUri(basePath, self.options)))
        .applyInternal(View.asSingleton())

    /**
     * Create a SideInput of `SparkeyReader` from a [[SparkeyUri]] base path, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     * If the provided base path ends with "*", it will be treated as a sharded collection of
     * Sparkey files.
     */
    @experimental
    def sparkeySideInput(basePath: String): SideInput[SparkeyReader] = {
      val view = if (basePath.endsWith("*")) {
        val basePathWithoutGlobPart = basePath.split("/").dropRight(1).mkString("/")
        shardedViewOf(basePathWithoutGlobPart)
      } else {
        singleViewOf(basePath)
      }

      new SparkeySideInput(view)
    }

    /**
     * Create a SideInput of `TypedSparkeyReader` from a [[SparkeyUri]] base path, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     * The provided decoder function will map from the underlying byte array to a JVM type, and
     * the optional [[Cache]] object can be used to cache reads in memory after decoding.
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

  private val DefaultNumShards: Short = 1
  private val DefaultSideInputNumShards: Short = 64

  private def writeToSparkey[K, V](
    uri: SparkeyUri,
    maxMemoryUsage: Long,
    elements: Iterable[(K, V)]
  )(implicit w: SparkeyWritable[K, V], koder: Coder[K], voder: Coder[V]): SparkeyUri = {
    val writer = new SparkeyWriter(uri, maxMemoryUsage)
    val it = elements.iterator
    while (it.hasNext) {
      val kv = it.next()
      w.put(writer, kv._1, kv._2)
    }
    writer.close()
    uri
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Sparkey methods. */
  implicit class SparkeyPairSCollection[K, V](@transient private val self: SCollection[(K, V)]) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * Write the key-value pairs of this SCollection as a Sparkey file to a specific location.
     *
     * @param path where to write the sparkey files. Defaults to a temporary location.
     * @param maxMemoryUsage (optional) how much memory (in bytes) is allowed for writing
     *                       the index file
     * @param numShards (optional) the number of shards to split this dataset into before writing.
     *                  One pair of Sparkey files will be written for each shard, sharded
     *                  by MurmurHash3 of the key mod the number of shards.
     * @return A singleton SCollection containing the [[SparkeyUri]] of the saved files.
     */
    @experimental
    def asSparkey(
      path: String = null,
      maxMemoryUsage: Long = -1,
      numShards: Short = DefaultNumShards
    )(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SCollection[SparkeyUri] = {
      require(numShards > 0, s"numShards must be greater than 0, found $numShards")

      val tempLocation = self.context.options.getTempLocation()
      val tempPath = s"$tempLocation/sparkey-${UUID.randomUUID}"
      val basePath = if (path == null) tempPath else path
      val nonShardedUri = SparkeyUri(basePath, self.context.options)
      require(!nonShardedUri.exists, s"Sparkey URI $nonShardedUri already exists")
      val uri = ShardedSparkeyUri(basePath, self.context.options)
      require(!uri.exists, s"Sparkey URI $uri already exists")

      logger.info(s"Saving as Sparkey with $numShards shards: $basePath")

      self.transform { collection =>
        val shards = collection
          .groupBy { case (k, _) => floorMod(w.shardHash(k), numShards).toShort }
          .map {
            case (shard, xs) =>
              shard -> writeToSparkey(
                uri.sparkeyUriForShard(shard, numShards),
                maxMemoryUsage,
                xs
              )
          }

        val shardsMap = shards.asMapSideInput

        val uris = shards.context
          .parallelize((0 until numShards).map(_.toShort))
          .withSideInputs(shardsMap)
          .map {
            case (shard, sideContext) =>
              sideContext(shardsMap).getOrElse(
                shard,
                writeToSparkey(
                  uri.sparkeyUriForShard(shard, numShards),
                  maxMemoryUsage,
                  Iterable.empty[(K, V)]
                )
              )
          }
          .toSCollection

        uris.reifyAsListInGlobalWindow
          .map { _ =>
            if (numShards == 1) {
              val src = FileSystems
                .`match`(basePath + "/*")
                .metadata()
                .asScala
                .map(_.resourceId())
                .sortWith(_.getFilename < _.getFilename)
                .asJava
              val dst = SparkeyUri.extensions
                .map(ext => FileSystems.matchNewResource(s"$basePath$ext", false))
                .asJava

              FileSystems.rename(src, dst)

              nonShardedUri
            } else {
              uri
            }

          }
      }
    }

    /**
     * Write the key-value pairs of this SCollection as a Sparkey file to a temporary location.
     *
     * @return A singleton SCollection containing the [[SparkeyUri]] of the saved files.
     */
    @experimental
    def asSparkey(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SCollection[SparkeyUri] = this.asSparkey()

    /**
     * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
     * `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. It is
     * required that each key of the input be associated with a single value.
     *
     * @param numShards the number of shards to use when writing the Sparkey file(s).
     */
    @experimental
    def asSparkeySideInput(numShards: Short = DefaultSideInputNumShards)(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SideInput[SparkeyReader] =
      self.asSparkey(numShards = numShards).asSparkeySideInput

    /**
     * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
     * `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. It is
     * required that each key of the input be associated with a single value.
     */
    @experimental
    def asSparkeySideInput(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SideInput[SparkeyReader] =
      self.asSparkeySideInput()

    /**
     * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
     * `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. It is
     * required that each key of the input be associated with a single value. The provided
     * [[Cache]] will be used to cache reads from the resulting [[SparkeyReader]].
     */
    @experimental
    def asTypedSparkeySideInput[T](decoder: Array[Byte] => T)(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SideInput[TypedSparkeyReader[T]] =
      self.asSparkey.asTypedSparkeySideInput[T](decoder)

    /**
     * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
     * `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]. It is
     * required that each key of the input be associated with a single value. The provided
     * [[Cache]] will be used to cache reads from the resulting [[SparkeyReader]].
     */
    @experimental
    def asTypedSparkeySideInput[T](
      cache: Cache[String, T],
      numShards: Short = DefaultSideInputNumShards
    )(
      decoder: Array[Byte] => T
    )(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SideInput[TypedSparkeyReader[T]] =
      self.asSparkey(numShards = numShards).asTypedSparkeySideInput[T](cache)(decoder)

    /**
     * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
     * `CachedStringSparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     */
    @experimental
    def asCachedStringSparkeySideInput(
      cache: Cache[String, String],
      numShards: Short = DefaultSideInputNumShards
    )(implicit
      w: SparkeyWritable[K, V],
      koder: Coder[K],
      voder: Coder[V]
    ): SideInput[CachedStringSparkeyReader] =
      self.asSparkey(numShards = numShards).asCachedStringSparkeySideInput(cache)
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Sparkey methods. */
  implicit class SparkeySCollection(private val self: SCollection[SparkeyUri]) extends AnyVal {

    /**
     * Convert this SCollection to a SideInput of `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     */
    @experimental
    def asSparkeySideInput: SideInput[SparkeyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }

    /**
     * Convert this SCollection to a SideInput of `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     * The provided decoder function will map from the underlying byte array to a JVM type, and
     * the optional [[Cache]] object can be used to cache reads in memory after decoding.
     */
    @experimental
    def asTypedSparkeySideInput[T](cache: Cache[String, T])(
      decoder: Array[Byte] => T
    ): SideInput[TypedSparkeyReader[T]] =
      asSparkeySideInput
        .map(reader => new TypedSparkeyReader[T](reader, decoder, cache))

    /**
     * Convert this SCollection to a SideInput of `SparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     * The provided decoder function will map from the underlying byte array to a JVM type, and
     * the optional [[Cache]] object can be used to cache reads in memory after decoding.
     */
    @experimental
    def asTypedSparkeySideInput[T](decoder: Array[Byte] => T): SideInput[TypedSparkeyReader[T]] =
      asSparkeySideInput
        .map(reader => new TypedSparkeyReader[T](reader, decoder, Cache.noOp))

    /**
     * Convert this SCollection to a SideInput of `CachedStringSparkeyReader`, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]].
     */
    @experimental
    def asCachedStringSparkeySideInput(
      cache: Cache[String, String]
    ): SideInput[CachedStringSparkeyReader] =
      asSparkeySideInput
        .map(reader => new CachedStringSparkeyReader(reader, cache))
  }

  /**
   * A wrapper class around SparkeyReader that allows the reading of multiple Sparkey files,
   * sharded by their keys (via MurmurHash3). At most 32,768 Sparkey files are supported.
   * @param sparkeys a map of shard ID to sparkey reader
   * @param numShards the total count of shards used (needed for keying as some shards may be empty)
   */
  class ShardedSparkeyReader(val sparkeys: Map[Short, SparkeyReader], val numShards: Short)
      extends SparkeyReader {
    def hashKey(arr: Array[Byte]): Short =
      floorMod(MurmurHash3.bytesHash(arr, 1), numShards).toShort

    def hashKey(str: String): Short =
      floorMod(MurmurHash3.stringHash(str, 1), numShards).toShort

    override def getAsString(key: String): String = {
      val hashed = hashKey(key)
      if (sparkeys.contains(hashed)) {
        sparkeys(hashed).getAsString(key)
      } else {
        null
      }
    }

    override def getAsByteArray(key: Array[Byte]): Array[Byte] = {
      val hashed = hashKey(key)
      if (sparkeys.contains(hashed)) {
        sparkeys(hashed).getAsByteArray(key)
      } else {
        null
      }
    }

    override def getAsEntry(key: Array[Byte]): SparkeyReader.Entry = {
      val hashed = hashKey(key)
      if (sparkeys.contains(hashed)) {
        sparkeys(hashed).getAsEntry(key)
      } else {
        null
      }
    }

    override def getIndexHeader: IndexHeader =
      throw new NotImplementedError("ShardedSparkeyReader does not support getIndexHeader.")

    override def getLogHeader: LogHeader =
      throw new NotImplementedError("ShardedSparkeyReader does not support getLogHeader.")

    override def duplicate(): SparkeyReader =
      new ShardedSparkeyReader(sparkeys.map { case (k, v) => (k, v.duplicate) }, numShards)

    override def close(): Unit = sparkeys.values.foreach(_.close())

    override def iterator(): JIterator[SparkeyReader.Entry] =
      sparkeys.values.map(_.iterator.asScala).reduce(_ ++ _).asJava
  }

  private class SparkeySideInput(val view: PCollectionView[SparkeyUri])
      extends SideInput[SparkeyReader] {
    override def updateCacheOnGlobalWindow: Boolean = false
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      context.sideInput(view).getReader
  }

  sealed trait SparkeyWritable[K, V] extends Serializable {
    private[sparkey] def put(w: SparkeyWriter, key: K, value: V): Unit
    private[sparkey] def shardHash(key: K): Int
  }

  implicit val stringSparkeyWritable: SparkeyWritable[String, String] =
    new SparkeyWritable[String, String] {
      def put(w: SparkeyWriter, key: String, value: String): Unit =
        w.put(key, value)

      def shardHash(key: String): Int = MurmurHash3.stringHash(key, 1)
    }

  implicit val ByteArraySparkeyWritable: SparkeyWritable[Array[Byte], Array[Byte]] =
    new SparkeyWritable[Array[Byte], Array[Byte]] {
      def put(w: SparkeyWriter, key: Array[Byte], value: Array[Byte]): Unit =
        w.put(key, value)

      def shardHash(key: Array[Byte]): Int = MurmurHash3.bytesHash(key, 1)
    }
}
