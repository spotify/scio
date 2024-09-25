/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.snowflake.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.snowflake.{SnowflakeOptions, SnowflakeSelect}
import com.spotify.scio.values.SCollection
import kantan.csv.RowDecoder

/** Enhanced version of [[ScioContext]] with Snowflake methods. */
final class SnowflakeScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a Snowflake SQL query
   *
   * @param snowflakeOptions
   *   options for configuring a Snowflake connexion
   * @param query
   *   Snowflake SQL select query
   */
  def snowflakeQuery[T: RowDecoder: Coder](
    snowflakeOptions: SnowflakeOptions,
    query: String
  ): SCollection[T] =
    self.read(SnowflakeSelect(snowflakeOptions, query))

}
trait ScioContextSyntax {
  implicit def snowflakeScioContextOps(sc: ScioContext): SnowflakeScioContextOps =
    new SnowflakeScioContextOps(sc)
}
