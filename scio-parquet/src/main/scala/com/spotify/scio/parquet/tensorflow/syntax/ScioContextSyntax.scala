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

package com.spotify.scio.parquet.tensorflow.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO.ReadParam
import com.spotify.scio.values.SCollection
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.tensorflow.proto.example.Example

/** Enhanced version of [[ScioContext]] with Parquet [[Example]] methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {

  /** Get an SCollection for a Parquet file as [[Example]] records. */
  def parquetExampleFile(
    path: String,
    projection: Seq[String] = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix
  ): SCollection[Example] =
    self.read(ParquetExampleIO(path))(
      ParquetExampleIO.ReadParam(projection, predicate, conf, suffix)
    )
}

trait ScioContextSyntax {
  implicit def parquetExampleScioContextOps(c: ScioContext): ScioContextOps = new ScioContextOps(c)
}
