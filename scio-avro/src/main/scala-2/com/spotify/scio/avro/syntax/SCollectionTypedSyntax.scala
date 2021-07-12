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

package com.spotify.scio.avro.syntax

import com.spotify.scio.avro._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values._
import org.apache.avro.file.CodecFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final class TypedAvroSCollectionOps[T <: HasAvroAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  def saveAsTypedAvroFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = AvroIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata
  )(implicit ct: ClassTag[T], tt: TypeTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(AvroTyped.AvroIO[T](path))(param)
  }
}

trait SCollectionTypedSyntax {
  implicit def avroTypedAvroSCollectionOps[T <: HasAvroAnnotation](
    c: SCollection[T]
  ): TypedAvroSCollectionOps[T] = new TypedAvroSCollectionOps[T](c)
}
