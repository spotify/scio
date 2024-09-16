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
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT}
import com.spotify.scio.values.SCollection
import magnolify.beam.RowType
import org.apache.beam.sdk.managed.Managed
import com.spotify.scio.managed.ManagedIO
import org.apache.beam.sdk.coders.RowCoder
import org.apache.beam.sdk.values.Row

final case class IcebergIO[T: RowType: Coder](table: String, catalogName: Option[String])
    extends ScioIO[T] {
  override type ReadP = IcebergIO.ReadParam
  override type WriteP = IcebergIO.WriteParam
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  private lazy val rowType: RowType[T] = implicitly
  private lazy val beamRowCoder: RowCoder = RowCoder.of(rowType.schema)
  implicit private lazy val rowCoder: Coder[Row] = Coder.beam(beamRowCoder)

  override def testId: String = s"IcebergIO($table, $catalogName)"

  private def config(
    catalogProperties: Map[String, String],
    configProperties: Map[String, String]
  ): Map[String, AnyRef] = {
    Map[String, AnyRef](
      "table" -> table,
      "catalog_name" -> catalogName.orNull,
      "catalog_properties" -> catalogProperties,
      "config_properties" -> configProperties
    ).filter(_._2 != null)
  }

  override protected def read(sc: ScioContext, params: IcebergIO.ReadParam): SCollection[T] =
    ManagedIO(Managed.ICEBERG, config(params.catalogProperties, params.configProperties))
      .readWithContext(sc, ManagedIO.ReadParam(rowType.schema))
      .map(rowType.from)

  override protected def write(data: SCollection[T], params: IcebergIO.WriteParam): Tap[tapT.T] = {
    val tx = data.transform {
      _.map(rowType.to).setCoder(beamRowCoder)
    }
    ManagedIO(Managed.ICEBERG, config(params.catalogProperties, params.configProperties))
      .writeWithContext(tx, ManagedIO.WriteParam())
      .underlying
}

  override def tap(read: IcebergIO.ReadParam): Tap[tapT.T] = EmptyTap
}

object IcebergIO {
  case class ReadParam private (
    catalogProperties: Map[String, String] = null,
    configProperties: Map[String, String] = null
  )
  case class WriteParam private (
    catalogProperties: Map[String, String] = null,
    configProperties: Map[String, String] = null
  )
}
