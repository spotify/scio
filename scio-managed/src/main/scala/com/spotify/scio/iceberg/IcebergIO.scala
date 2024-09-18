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

  override def testId: String = s"IcebergIO(${(Some(table) ++ catalogName).mkString(", ")})"

  private def config(
    catalogProperties: Map[String, String],
    configProperties: Map[String, String]
  ): Map[String, AnyRef] = {
    val b = Map.newBuilder[String, AnyRef]
    b.addOne("table" -> table)
    catalogName.foreach(name => b.addOne("catalog_name" -> name))
    Option(catalogProperties).foreach(p => b.addOne("catalog_properties" -> p))
    Option(configProperties).foreach(p => b.addOne("config_properties" -> p))
    b.result()
  }

  override protected def read(sc: ScioContext, params: IcebergIO.ReadParam): SCollection[T] = {
    val io = ManagedIO(Managed.ICEBERG, config(params.catalogProperties, params.configProperties))
    sc.transform(_.read(io)(ManagedIO.ReadParam(rowType.schema)).map(rowType.from))
  }

  override protected def write(data: SCollection[T], params: IcebergIO.WriteParam): Tap[tapT.T] = {
    val io = ManagedIO(Managed.ICEBERG, config(params.catalogProperties, params.configProperties))
    data.map(rowType.to).setCoder(beamRowCoder).write(io).underlying
  }

  override def tap(read: IcebergIO.ReadParam): Tap[tapT.T] = EmptyTap
}

object IcebergIO {
  case class ReadParam private (
    catalogProperties: Map[String, String] = ReadParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = ReadParam.DefaultConfigProperties
  )
  object ReadParam {
    val DefaultCatalogProperties: Map[String, String] = null
    val DefaultConfigProperties: Map[String, String] = null
  }
  case class WriteParam private (
    catalogProperties: Map[String, String] = WriteParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = WriteParam.DefaultConfigProperties
  )
  object WriteParam {
    val DefaultCatalogProperties: Map[String, String] = null
    val DefaultConfigProperties: Map[String, String] = null
  }
}
