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

package com.spotify.scio

import java.sql.{Driver, PreparedStatement, ResultSet}

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.io.{jdbc => beam}

import scala.reflect.ClassTag

/**
 * Main package for JDBC APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.jdbc._
 * }}}
 */
package object jdbc {

  /** Enhanced version of [[ScioContext]] with JDBC methods. */
  implicit class JdbcScioContext(@transient private val self: ScioContext) extends AnyVal {

    /** Get an SCollection for a JDBC query. */
    def jdbcSelect[T: ClassTag: Coder](readOptions: JdbcReadOptions[T]): SCollection[T] =
      self.read(JdbcSelect(readOptions))
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
  implicit class JdbcSCollection[T](@transient private val self: SCollection[T]) extends AnyVal {

    /** Save this SCollection as a JDBC database. */
    def saveAsJdbc(
      writeOptions: JdbcWriteOptions[T]
    )(implicit coder: Coder[T]): ClosedTap[Nothing] =
      self.write(JdbcWrite(writeOptions))
  }

}
