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

package com.spotify.scio.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.values.SCollection
import magnolify.avro.{AvroType => MagnolifyAvroType}
import org.apache.avro.generic.GenericRecord

import scala.reflect.runtime.universe._

final case class AvroTypedIO[T <: HasAvroAnnotation: TypeTag: Coder](path: String)
    extends ScioIO[T] {
  override type ReadP = AvroTypedIO.ReadParam
  override type WriteP = AvroTypedIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"AvroIO($path)"

  private val avroT = AvroType[T]
  private lazy val schema = avroT.schema
  private lazy val underlying: GenericRecordIO = GenericRecordIO(path, schema)

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params).map(avroT.fromGenericRecord)

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val datumFactory = Option(params.datumFactory).getOrElse(GenericRecordDatumFactory)
    implicit val coder: Coder[GenericRecord] = avroCoder(datumFactory, schema)
    data.map(avroT.toGenericRecord).write(underlying)(params)
    tap(AvroIO.ReadParam(params))
  }

  override def tap(read: ReadP): Tap[T] =
    underlying.tap(read).map(avroT.fromGenericRecord)
}

object AvroTypedIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}

@deprecated("Use AvroTypedIO instead", "0.14.0")
object AvroTyped {
  type AvroIO[T <: HasAvroAnnotation] = AvroTypedIO[T]
  def AvroIO[T <: HasAvroAnnotation: TypeTag: Coder](path: String): AvroIO[T] = AvroTypedIO[T](path)
}

final case class AvroMagnolifyTyped[T: MagnolifyAvroType: Coder](path: String) extends ScioIO[T] {
  override type ReadP = AvroMagnolifyTyped.ReadParam
  override type WriteP = AvroMagnolifyTyped.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"AvroIO($path)"

  private lazy val avroT: MagnolifyAvroType[T] = implicitly
  private lazy val schema = avroT.schema
  private lazy val underlying: GenericRecordIO = GenericRecordIO(path, schema)

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params).map(avroT.from)

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val datumFactory = Option(params.datumFactory).getOrElse(GenericRecordDatumFactory)
    implicit val coder: Coder[GenericRecord] = avroCoder(datumFactory, schema)
    data.map(avroT.to).write(underlying)(params)
    tap(AvroIO.ReadParam(params))
  }

  override def tap(read: ReadP): Tap[T] = underlying.tap(read).map(avroT.from)
}

object AvroMagnolifyTyped {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}
