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

package com.spotify.scio.parquet

import com.spotify.scio.parquet.avro.syntax.Syntax

/**
 * Main package for Parquet Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.avro._
 * }}}
 */
package object avro extends Syntax {

  /** Alias for `me.lyh.parquet.avro.Projection`. */
  val Projection = me.lyh.parquet.avro.Projection

  /** Alias for `me.lyh.parquet.avro.Predicate`. */
  val Predicate = me.lyh.parquet.avro.Predicate
}
