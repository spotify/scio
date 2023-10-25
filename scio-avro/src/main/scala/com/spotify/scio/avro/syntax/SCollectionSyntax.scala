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

import com.google.protobuf.Message
import com.spotify.scio.avro._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.util.{FilenamePolicySupplier, ScioUtil}
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.avro.io.AvroSource
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final class AvroReadFilesSCollectionOps(private val self: SCollection[String]) extends AnyVal {
  def readFilesAsAvroWithFilename[T <: SpecificRecord: Coder: ClassTag](
    emptyMatchTreatment: EmptyMatchTreatment = EmptyMatchTreatment.DISALLOW
  ): SCollection[(String, T)] = {
    val coder = CoderMaterializer.beam(self.context, Coder[T])
    self.readFilesWithFilename[T](fn =>
      AvroSource
        .from(fn)
        .withSchema[T](ScioUtil.classOf[T])
        .withEmptyMatchTreatment(emptyMatchTreatment)
        .withCoder(coder)
    )
  }

  def readFilesAsGenericRecordWithFilename(
    schema: Schema,
    emptyMatchTreatment: EmptyMatchTreatment = EmptyMatchTreatment.DISALLOW
  ): SCollection[(String, GenericRecord)] = {
    val coder = CoderMaterializer.beam(self.context, Coder.avroGenericRecordCoder(schema))
    val schemaStr = schema.toString
    self.readFilesWithFilename[GenericRecord](fn =>
      AvroSource
        .from(fn)
        .withSchema(new Schema.Parser().parse(schemaStr))
        .withEmptyMatchTreatment(emptyMatchTreatment)
        .withCoder(coder)
    )
  }
}

final class GenericRecordSCollectionOps(private val self: SCollection[GenericRecord])
    extends AnyVal {

  /**
   * Save this SCollection of type [[org.apache.avro.generic.GenericRecord GenericRecord]] as an
   * Avro file.
   */
  def saveAsAvroFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    schema: Schema,
    suffix: String = AvroIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  ): ClosedTap[GenericRecord] = {
    val param = AvroIO.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(GenericRecordIO(path, schema))(param)
  }
}

final class ObjectFileSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  def saveAsObjectFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = ".obj.avro",
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(implicit coder: Coder[T]): ClosedTap[T] = {
    val param = ObjectFileIO.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(ObjectFileIO(path))(param)
  }
}

final class SpecificRecordSCollectionOps[T <: SpecificRecord](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection of type [[org.apache.avro.specific.SpecificRecord SpecificRecord]] as an
   * Avro file.
   */
  def saveAsAvroFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = AvroIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = AvroIO.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(SpecificRecordIO[T](path))(param)
  }
}

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
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(implicit ct: ClassTag[T], tt: TypeTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = AvroIO.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(AvroTyped.AvroIO[T](path))(param)
  }
}

final class ProtobufSCollectionOps[T <: Message](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  def saveAsProtobufFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = ".protobuf.avro",
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = AvroIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      AvroIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = ProtobufIO.WriteParam(
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(ProtobufIO[T](path))(param)
  }
}

/** Enhanced with Avro methods. */
trait SCollectionSyntax {
  implicit def avroReadFilesSCollectionOps(
    c: SCollection[String]
  ): AvroReadFilesSCollectionOps = new AvroReadFilesSCollectionOps(c)

  implicit def avroGenericRecordSCollectionOps(
    c: SCollection[GenericRecord]
  ): GenericRecordSCollectionOps = new GenericRecordSCollectionOps(c)

  implicit def avroObjectFileSCollectionOps[T](
    c: SCollection[T]
  ): ObjectFileSCollectionOps[T] = new ObjectFileSCollectionOps[T](c)

  implicit def avroSpecificRecordSCollectionOps[T <: SpecificRecord](
    c: SCollection[T]
  ): SpecificRecordSCollectionOps[T] = new SpecificRecordSCollectionOps[T](c)

  implicit def avroTypedAvroSCollectionOps[T <: HasAvroAnnotation](
    c: SCollection[T]
  ): TypedAvroSCollectionOps[T] = new TypedAvroSCollectionOps[T](c)

  implicit def avroProtobufSCollectionOps[T <: Message](
    c: SCollection[T]
  ): ProtobufSCollectionOps[T] = new ProtobufSCollectionOps[T](c)
}
