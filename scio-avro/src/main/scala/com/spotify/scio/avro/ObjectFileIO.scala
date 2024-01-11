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
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

final case class ObjectFileIO[T: Coder](
  path: String,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends ScioIO[T] {
  override type ReadP = ObjectFileIO.ReadParam
  override type WriteP = ObjectFileIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"ObjectFileIO($path)"

  private lazy val underlying: GenericRecordIO =
    GenericRecordIO(path, AvroBytesUtil.schema, datumFactory)

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val objectCoder = CoderMaterializer.beamWithDefault(Coder[T])
    sc.read(underlying)(params).map(record => AvroBytesUtil.decode(objectCoder, record))
  }

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val objectCoder = CoderMaterializer.beamWithDefault(Coder[T])
    implicit val avroCoder: Coder[GenericRecord] = avroGenericRecordCoder(AvroBytesUtil.schema)
    data.map(element => AvroBytesUtil.encode(objectCoder, element)).write(underlying)(params)
    tap(AvroIO.ReadParam(params))
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap(path, read)
}

object ObjectFileIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}
