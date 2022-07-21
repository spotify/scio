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

package com.spotify.scio.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag


final case class ObjectFileIO[T: Coder](path: String) extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = ObjectFileIO.WriteParam
  final override val tapT: TapT.Aux[T, T] = TapOf[T]

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beamWithDefault(Coder[T])
    sc.read(GenericRecordIO(path, AvroBytesUtil.schema))
      .parDo(new DoFn[GenericRecord, T] {
        @ProcessElement
        private[scio] def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit =
          c.output(AvroBytesUtil.decode(coder, c.element()))
      })
  }

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val elemCoder = CoderMaterializer.beamWithDefault(Coder[T])
    implicit val bcoder = Coder.avroGenericRecordCoder(AvroBytesUtil.schema)
    data
      .parDo(new DoFn[T, GenericRecord] {
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, GenericRecord]#ProcessContext): Unit =
          c.output(AvroBytesUtil.encode(elemCoder, c.element()))
      })
      .write(GenericRecordIO(path, AvroBytesUtil.schema))(params)
    tap(())
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))
}

object ObjectFileIO {
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}
