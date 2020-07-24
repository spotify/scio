/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.jdbc.sharded

import com.spotify.scio.ScioContext
import com.spotify.scio.coders._
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Read
import com.spotify.scio.io.TapT

final case class JdbcShardedSelect[T: Coder, S](
  readOptions: JdbcShardedReadOptions[T, S]
) extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcShardedSelect(${JdbcShardedSelect.jdbcIoId(readOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {

    val transform = Read.from(
      new JdbcShardedSource(readOptions, CoderMaterializer.beam(sc, Coder[T]))
    )

    sc.applyTransform(transform)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("JdbcShardedSelect is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object JdbcShardedSelect {

  private[jdbc] def jdbcIoId[T, S](opts: JdbcShardedReadOptions[T, S]): String =
    s"${opts.connectionOptions.connectionUrl}-${opts.tableName}"
}
