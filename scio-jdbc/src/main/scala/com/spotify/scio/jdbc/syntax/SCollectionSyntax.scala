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

package com.spotify.scio.jdbc.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc.JdbcWriteOptions
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.jdbc.JdbcWrite

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
final class JdbcSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /** Save this SCollection as a JDBC database. */
  def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): ClosedTap[Nothing] =
    self.write(JdbcWrite(writeOptions))
}

trait SCollectionSyntax {
  implicit def jdbcSCollectionOps[T](sc: SCollection[T]): JdbcSCollectionOps[T] =
    new JdbcSCollectionOps(sc)
}
