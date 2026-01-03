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
import magnolia1._

private[scio] object ConfigMap {
  type Typeclass[T] = ConfigMapType[T]

  trait ConfigMapType[T] {
    def toMap(value: T): Map[String, AnyRef]
  }
  implicit def gen[T]: ConfigMapType[T] = macro Magnolia.gen[T]

  // just needed to satisfy magnolia
  implicit val stringToMap: ConfigMapType[String] = _ => Map.empty
  implicit val intToMap: ConfigMapType[Int] = _ => Map.empty
  implicit val mapToMap: ConfigMapType[Map[String, String]] = _ => Map.empty
  implicit val listToMap: ConfigMapType[List[String]] = _ => Map.empty
  implicit def optionToMap[T]: ConfigMapType[Option[T]] = _ => Map.empty

  private def toSnakeCase(s: String): String =
    s.replaceAll("([^A-Z])([A-Z])", "$1_$2").toLowerCase

  def join[T](caseClass: CaseClass[ConfigMapType, T]): ConfigMapType[T] = (value: T) => {
    caseClass.parameters.flatMap { p =>
      val label = toSnakeCase(p.label)
      val fieldValue = p.dereference(value)
      fieldValue match {
        case null         => None
        case x: Option[_] => x.map(v => label -> v.asInstanceOf[AnyRef])
        case _            =>
          Some(label -> fieldValue.asInstanceOf[AnyRef])
      }
    }.toMap
  }
}

final case class IcebergIO[T: RowType: Coder](table: String, catalogName: Option[String])
    extends ScioIO[T] {
  override type ReadP = IcebergIO.ReadParam
  override type WriteP = IcebergIO.WriteParam
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  private lazy val rowType: RowType[T] = implicitly
  private lazy val beamRowCoder: RowCoder = RowCoder.of(rowType.schema)
  implicit private lazy val rowCoder: Coder[Row] = Coder.beam(beamRowCoder)

  override def testId: String = s"IcebergIO(${(Some(table) ++ catalogName).mkString(", ")})"

  private[scio] def config[P](
    params: P
  )(implicit mapper: ConfigMap.ConfigMapType[P]): Map[String, AnyRef] = {
    val b = Map.newBuilder[String, AnyRef]
    b += ("table" -> table)
    catalogName.foreach(name => b += ("catalog_name" -> name))
    b ++= mapper.toMap(params)
    b.result()
  }

  override protected def read(sc: ScioContext, params: IcebergIO.ReadParam): SCollection[T] = {
    val io = ManagedIO(Managed.ICEBERG, config(params))
    sc.transform(_.read(io)(ManagedIO.ReadParam(rowType.schema)).map(rowType.from))
  }

  override protected def write(data: SCollection[T], params: IcebergIO.WriteParam): Tap[tapT.T] = {
    val io = ManagedIO(Managed.ICEBERG, config(params))
    data.map(rowType.to).setCoder(beamRowCoder).write(io).underlying
  }

  override def tap(read: IcebergIO.ReadParam): Tap[tapT.T] = EmptyTap
}

object IcebergIO {
  case class ReadParam private (
    catalogProperties: Map[String, String] = ReadParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = ReadParam.DefaultConfigProperties,
    keep: List[String] = ReadParam.DefaultKeep,
    drop: List[String] = ReadParam.DefaultDrop,
    filter: String = ReadParam.DefaultFilter
  )
  object ReadParam {
    val DefaultCatalogProperties: Map[String, String] = null
    val DefaultConfigProperties: Map[String, String] = null
    val DefaultKeep: List[String] = null
    val DefaultDrop: List[String] = null
    val DefaultFilter: String = null

    implicit val configMap: ConfigMap.ConfigMapType[ReadParam] = ConfigMap.gen[ReadParam]
  }
  case class WriteParam private (
    catalogProperties: Map[String, String] = WriteParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = WriteParam.DefaultConfigProperties,
    triggeringFrequencySeconds: Option[Int] = None,
    directWriteByteLimit: Option[Int] = None,
    keep: List[String] = WriteParam.DefaultKeep,
    drop: List[String] = WriteParam.DefaultDrop,
    only: String = WriteParam.DefaultOnly
  )
  object WriteParam {
    val DefaultCatalogProperties: Map[String, String] = null
    val DefaultConfigProperties: Map[String, String] = null
    val DefaultTriggeringFrequencySeconds: Int = -1
    val DefaultDirectWriteByteLimit: Int = -1
    val DefaultKeep: List[String] = null
    val DefaultDrop: List[String] = null
    val DefaultOnly: String = null

    implicit val configMap: ConfigMap.ConfigMapType[WriteParam] = ConfigMap.gen[WriteParam]
  }
}
