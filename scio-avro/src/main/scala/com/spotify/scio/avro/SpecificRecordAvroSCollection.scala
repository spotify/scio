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

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.avro.file.CodecFactory
import org.apache.avro.specific.SpecificRecordBase

import scala.concurrent.Future
import scala.reflect.ClassTag

final class SpecificRecordAvroSCollection[T <: SpecificRecordBase](
  @transient val self: SCollection[T])
    extends {

  /**
   * Save this SCollection of type
   * [[org.apache.avro.specific.SpecificRecordBase SpecificRecordBase]] as an Avro file.
   *
   */
  def saveAsAvroFile(path: String,
                     numShards: Int = AvroIO.WriteParam.DefaultNumShards,
                     suffix: String = AvroIO.WriteParam.DefaultSuffix,
                     codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
                     metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata)(
    implicit ct: ClassTag[T],
    coder: Coder[T]): Future[Tap[T]] = {
    val param = AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(SpecificRecordIO[T](path))(param)
  }
}
