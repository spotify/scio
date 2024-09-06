/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.iceberg

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{EmptyTapOf, ScioIO, Tap, TapT}
import com.spotify.scio.values.SCollection
import magnolify.beam.RowType
import org.apache.beam.sdk.managed.Managed
import com.spotify.scio.managed.ManagedIO
import org.apache.beam.sdk.values.Row

final case class IcebergIO[T : RowType : Coder](config: Map[String, AnyRef]) extends ScioIO[T] {
  override type ReadP = IcebergIO.ReadParam
  override type WriteP = IcebergIO.WriteParam
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  private lazy val rowType: RowType[T] = implicitly
  private lazy val underlying: ManagedIO = ManagedIO(Managed.ICEBERG, config)
  private lazy implicit val rowCoder: Coder[Row] = Coder.row(rowType.schema)

  override protected def read(sc: ScioContext, params: IcebergIO.ReadParam): SCollection[T] =
    underlying.readWithContext(sc, params).map(rowType.from)

  override protected def write(data: SCollection[T], params: IcebergIO.WriteParam): Tap[tapT.T] =
    underlying.writeWithContext(data.transform(_.map(rowType.to)), params).underlying

  override def tap(read: IcebergIO.ReadParam): Tap[tapT.T] = underlying.tap(read)
}

object IcebergIO {
  type ReadParam = ManagedIO.ReadParam
  type WriteParam = ManagedIO.WriteParam
}
