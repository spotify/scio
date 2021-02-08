/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.parquet

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.types.ParquetTypeIO.WriteParam
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

/**
 * Main package for Parquet type-safe APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.types._
 * }}}
 */
package object types {

  /** Enhanced version of [[ScioContext]] with Parquet type-safe methods. */
  implicit class ParquetTypeScioContext(private val self: ScioContext) extends AnyVal {

    /** Get an SCollection for a Parquet file as case classes `T`. */
    def typedParquetFile[T: ClassTag: Coder: ParquetType](
      path: String,
      predicate: FilterPredicate = null
    ): SCollection[T] =
      self.read(ParquetTypeIO[T](path))(ParquetTypeIO.ReadParam(predicate))
  }

  /** Enhanced version of [[SCollection]] with Parquet type-safe methods. */
  implicit class ParquetTypeSCollection[T](private val self: SCollection[T]) extends AnyVal {

    /** Save this SColleciton of case classes `T` as a Parquet file. */
    def saveAsTypedParquetFile(
      path: String,
      numShards: Int = WriteParam.DefaultNumShards,
      suffix: String = WriteParam.DefaultSuffix,
      compression: CompressionCodecName = WriteParam.DefaultCompression
    )(implicit ct: ClassTag[T], coder: Coder[T], pt: ParquetType[T]): ClosedTap[T] =
      self.write(ParquetTypeIO[T](path))(WriteParam(numShards, suffix, compression))
  }
}
