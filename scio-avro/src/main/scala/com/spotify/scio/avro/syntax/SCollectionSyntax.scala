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
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.util.{FilenamePolicySupplier, ScioUtil}
import com.spotify.scio.values._
import magnolify.avro.{AvroType => AvroMagnolifyType}
import magnolify.protobuf.ProtobufType
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.avro.io.{AvroDatumFactory, AvroIO => BAvroIO, AvroSource}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final class GenericRecordSCollectionOps(private val self: SCollection[GenericRecord])
    extends AnyVal {

  /**
   * Save this SCollection of type [[org.apache.avro.generic.GenericRecord GenericRecord]] as an
   * Avro file.
   */
  def saveAsAvroFile(
    path: String,
    schema: Schema,
    numShards: Int = GenericRecordIO.WriteParam.DefaultNumShards,
    suffix: String = GenericRecordIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = GenericRecordIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = GenericRecordIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = GenericRecordIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = GenericRecordIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      GenericRecordIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = GenericRecordIO.WriteParam.DefaultPrefix,
    datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordIO.WriteParam.DefaultDatumFactory
  ): ClosedTap[GenericRecord] = {
    val param = GenericRecordIO.WriteParam(
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
    numShards: Int = ObjectFileIO.WriteParam.DefaultNumShards,
    suffix: String = ObjectFileIO.WriteParam.DefaultSuffixObjectFile,
    codec: CodecFactory = ObjectFileIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = ObjectFileIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = ObjectFileIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = ObjectFileIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      ObjectFileIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = ObjectFileIO.WriteParam.DefaultPrefix
  )(implicit coder: Coder[T]): ClosedTap[T] = {
    val param = ObjectFileIO.WriteParam[GenericRecord](
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(ObjectFileIO[T](path))(param)
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
    numShards: Int = SpecificRecordIO.WriteParam.DefaultNumShards,
    suffix: String = SpecificRecordIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = SpecificRecordIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = SpecificRecordIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = SpecificRecordIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = SpecificRecordIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      SpecificRecordIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = SpecificRecordIO.WriteParam.DefaultPrefix,
    datumFactory: AvroDatumFactory[T] = SpecificRecordIO.WriteParam.DefaultDatumFactory
  )(implicit ct: ClassTag[T]): ClosedTap[T] = {
    val param = SpecificRecordIO.WriteParam(
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
    self.write(SpecificRecordIO[T](path))(param)
  }
}

final class TypedAvroSCollectionOps[T <: HasAvroAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  @deprecated("Use magnolify API instead.", "0.15.0")
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
  )(implicit tt: TypeTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = AvroTypedIO.WriteParam(
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
    self.write(AvroTypedIO[T](path))(param)
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
    numShards: Int = ProtobufObjectFileIO.WriteParam.DefaultNumShards,
    suffix: String = ProtobufObjectFileIO.WriteParam.DefaultSuffixProtobuf,
    codec: CodecFactory = ProtobufObjectFileIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = ProtobufObjectFileIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = ProtobufObjectFileIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = ProtobufObjectFileIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      ProtobufObjectFileIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = ProtobufObjectFileIO.WriteParam.DefaultPrefix
  )(implicit ct: ClassTag[T]): ClosedTap[T] = {
    val param = ProtobufObjectFileIO.WriteParam[GenericRecord](
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(ProtobufObjectFileIO[T](path))(param)
  }
}

final class TypedMagnolifyProtobufSCollectionOps[T](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  def saveAsProtobufFile[U <: Message: ClassTag](
    path: String,
    numShards: Int = ProtobufTypedObjectFileIO.WriteParam.DefaultNumShards,
    suffix: String = ProtobufTypedObjectFileIO.WriteParam.DefaultSuffixProtobuf,
    codec: CodecFactory = ProtobufTypedObjectFileIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = ProtobufTypedObjectFileIO.WriteParam.DefaultMetadata,
    shardNameTemplate: String = ProtobufTypedObjectFileIO.WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = ProtobufTypedObjectFileIO.WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      ProtobufTypedObjectFileIO.WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = ProtobufTypedObjectFileIO.WriteParam.DefaultPrefix
  )(implicit pt: ProtobufType[T, U]): ClosedTap[T] = {
    implicit val tCoder: Coder[T] = self.coder
    val param = ProtobufTypedObjectFileIO.WriteParam[GenericRecord](
      numShards,
      suffix,
      codec,
      metadata,
      filenamePolicySupplier,
      prefix,
      shardNameTemplate,
      tempDirectory
    )
    self.write(ProtobufTypedObjectFileIO[T, U](path))(param)
  }
}

final class TypedMagnolifyAvroSCollectionOps[T](private val self: SCollection[T]) {

  def saveAsAvroFile(
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
  )(implicit coder: Coder[T], at: AvroMagnolifyType[T]): ClosedTap[T] = {
    val param = AvroMagnolifyTypedIO.WriteParam(
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
    self.write(AvroMagnolifyTypedIO[T](path))(param)
  }
}

final class FilesSCollectionOps(private val self: SCollection[String]) extends AnyVal {

  def readAvroGenericFiles(
    schema: Schema,
    datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordIO.ReadParam.DefaultDatumFactory
  ): SCollection[GenericRecord] = {
    val df = Option(datumFactory).getOrElse(GenericRecordDatumFactory)
    implicit val coder: Coder[GenericRecord] = avroCoder(df, schema)
    val transform = BAvroIO
      .readFilesGenericRecords(schema)
      .withDatumReaderFactory(df)
    self.readFiles(filesTransform = transform)
  }

  def readAvroSpecificFiles[T <: SpecificRecord: ClassTag](
    datumFactory: AvroDatumFactory[T] = SpecificRecordIO.ReadParam.DefaultDatumFactory
  ): SCollection[T] = {
    val recordClass = ScioUtil.classOf[T]
    val schema = SpecificData.get().getSchema(recordClass)
    val df = Option(datumFactory).getOrElse(new SpecificRecordDatumFactory(recordClass))
    implicit val coder: Coder[T] = avroCoder(df, schema)
    val transform = BAvroIO
      .readFiles(recordClass)
      .withDatumReaderFactory(df)
    self.readFiles(filesTransform = transform)
  }

  def readAvroGenericFilesWithPath(
    schema: Schema,
    datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordIO.ReadParam.DefaultDatumFactory
  ): SCollection[(String, GenericRecord)] = {
    val df = Option(datumFactory).getOrElse(GenericRecordDatumFactory)
    implicit val coder: Coder[GenericRecord] = avroCoder(df, schema)
    self.readFilesWithPath() { f =>
      AvroSource
        .from(f)
        .withSchema(schema)
        .withDatumReaderFactory(df)
    }
  }

  def readAvroSpecificFilesWithPath[T <: SpecificRecord: ClassTag](
    datumFactory: AvroDatumFactory[T] = SpecificRecordIO.ReadParam.DefaultDatumFactory
  ): SCollection[(String, T)] = {
    val recordClass = ScioUtil.classOf[T]
    val schema = SpecificData.get().getSchema(recordClass)
    val df = Option(datumFactory).getOrElse(new SpecificRecordDatumFactory(recordClass))
    implicit val coder: Coder[T] = avroCoder(df, schema)
    self.readFilesWithPath() { f =>
      AvroSource
        .from(f)
        .withSchema(recordClass)
        .withDatumReaderFactory(df)
    }
  }
}

/** Enhanced with Avro methods. */
trait SCollectionSyntax {
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

  implicit def avroFilesSCollectionOps[T](
    c: SCollection[T]
  )(implicit ev: T <:< String): FilesSCollectionOps =
    new FilesSCollectionOps(c.covary_)

  implicit def typedAvroProtobufSCollectionOps[T](
    c: SCollection[T]
  ): TypedMagnolifyProtobufSCollectionOps[T] = new TypedMagnolifyProtobufSCollectionOps[T](c)

  implicit def typedMagnolifyAvroSCollectionOps[T](
    c: SCollection[T]
  ): TypedMagnolifyAvroSCollectionOps[T] = new TypedMagnolifyAvroSCollectionOps(c)
}
