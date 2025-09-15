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

package com.spotify.scio.parquet.types.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.types.ParquetTypeIO
import com.spotify.scio.parquet.types.ParquetTypeIO.ReadParam
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate

import scala.reflect.ClassTag

/** Enhanced version of [[ScioContext]] with Parquet type-safe methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {

  /** Get an SCollection for a Parquet file as case classes `T`. */
  def typedParquetFile[T: ClassTag: Coder: ParquetType](
    path: String,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix
  ): SCollection[T] =
    self.read(ParquetTypeIO[T](path))(ParquetTypeIO.ReadParam(predicate, conf, suffix))
}
trait ScioContextSyntax {
  implicit def parquetTypeScioContext(c: ScioContext): ScioContextOps = new ScioContextOps(c)
}
