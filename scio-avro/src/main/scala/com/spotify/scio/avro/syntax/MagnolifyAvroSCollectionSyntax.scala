/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.avro.syntax

import com.spotify.scio.avro.{AvroMagnolifyTyped, AvroTypedIO}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import magnolify.avro.{AvroType => MagnolifyAvroType}
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

final class TypedMagnolifyAvroSCollectionOps[T: MagnolifyAvroType](
  private val
  self: SCollection[T]
) {

  def saveAsTypedAvroFile(
    path: String,
    numShards: Int = AvroTypedIO.WriteParam.DefaultNumShards,
    suffix: String = AvroTypedIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroTypedIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroTypedIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroTypedIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroTypedIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroTypedIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroTypedIO.WriteParam.DefaultPrefix,
    datumFactory: AvroDatumFactory[GenericRecord] = AvroTypedIO.WriteParam.DefaultDatumFactory
  )(implicit coder: Coder[T]): ClosedTap[T] = {
    val param = AvroMagnolifyTyped.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory,
      datumFactory
    )
    self.write(AvroMagnolifyTyped[T](path))(param)
  }
}

trait MagnolifyAvroSCollectionSyntax {

  implicit def typedMagnolifyAvroSCollectionOps[T: MagnolifyAvroType](
    c: SCollection[T]
  ): TypedMagnolifyAvroSCollectionOps[T] = new TypedMagnolifyAvroSCollectionOps(c)
}
