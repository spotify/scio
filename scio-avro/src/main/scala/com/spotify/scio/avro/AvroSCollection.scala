/*
 * Copyright 2018 Spotify AB.
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

import scala.concurrent.Future
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import com.google.protobuf.Message
import com.spotify.scio.io.Tap
import com.spotify.scio.util._
import com.spotify.scio.values._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation

import scala.concurrent._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[SCollection]] with Avro methods. */
final class AvroSCollection[T](@transient val self: SCollection[T]) extends Serializable {

  import self.{context, saveAsInMemoryTap, ct}

  /**
   * Extract data from this SCollection as a `Future`. The `Future` will be completed once the
   * pipeline completes successfully.
   * @group output
   */
  def materialize: Future[Tap[T]] = materialize(ScioUtil.getTempFile(context), isCheckpoint = false)
  private[scio] def materialize(path: String, isCheckpoint: Boolean): Future[Tap[T]] =
    if(context.isTest && isCheckpoint) {
      // if it's a test and checkpoint - no need to test checkpoint data
      ()
      saveAsInMemoryTap
    } else if(context.isTest) {
      // Do not run assertions on materilized value but still access test context to trigger
      // the test checking if we're running inside a JobTest
      context.testOutNio
      saveAsInMemoryTap
    } else {
      saveAsObjectFile(path)
    }

  /**
   * Save this SCollection as an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group output
   */
  def saveAsAvroFile(path: String,
                     numShards: Int = 0,
                     schema: Schema = null,
                     suffix: String = "",
                     codec: CodecFactory = CodecFactory.deflateCodec(6),
                     metadata: Map[String, AnyRef] = Map.empty)
  : Future[Tap[T]] = {
    val paramters = nio.AvroFile.Parameters(numShards, suffix, codec, metadata)
    self.write(nio.AvroFile[T](path, schema))(paramters)
  }

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   * @group output
   */
  def saveAsTypedAvroFile(path: String,
                          numShards: Int = 0,
                          suffix: String = "",
                          codec: CodecFactory = CodecFactory.deflateCodec(6),
                          metadata: Map[String, AnyRef] = Map.empty)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAvroAnnotation)
  : Future[Tap[T]] = {
    val parameters = nio.Typed.AvroFile.Parameters(numShards, suffix, codec, metadata)
    self.write(nio.Typed.AvroFile[T](path))(parameters)
  }

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group output
   */
  def saveAsObjectFile(path: String, numShards: Int = 0, suffix: String = ".obj",
                       metadata: Map[String, AnyRef] = Map.empty): Future[Tap[T]] =
    self.write(nio.ObjectFile[T](path))(nio.ObjectFile.Parameters(numShards, suffix, metadata))

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group output
   */
  def saveAsProtobufFile(path: String, numShards: Int = 0)
                        (implicit ev: T <:< Message): Future[Tap[T]] =
    self.write(nio.ProtobufFile[T](path))(nio.ProtobufFile.Parameters(numShards))
}
