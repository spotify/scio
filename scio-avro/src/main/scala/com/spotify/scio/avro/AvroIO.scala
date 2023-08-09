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

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.protobuf.util.ProtobufUtil
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.beam.sdk.extensions.avro.io.{AvroDatumFactory, AvroIO => BAvroIO}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining._

sealed trait AvroIO[T <: IndexedRecord] extends ScioIO[T] {
  override type ReadP = AvroIO.ReadParam
  override type WriteP = AvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  def path: String
  def schema: Schema

  protected def datumFactory: AvroDatumFactory[T]
  protected def baseRead: BAvroIO.Read[T]
  protected def baseWrite: BAvroIO.Write[T]

  implicit lazy val coder: Coder[T] = avroCoder(datumFactory, schema)
  override def testId: String = s"AvroIO($path)"
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val bCoder = CoderMaterializer.beam(sc, coder)
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val t = baseRead
      .from(filePattern)
      .withDatumReaderFactory(datumFactory)
    sc
      .applyTransform(t)
      .setCoder(bCoder)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val isWindowed = ScioUtil.isWindowed(data)
    val tempDirectory = ScioUtil.tempDirOrDefault(params.tempDirectory, data.context)
    val fp = FilenamePolicySupplier.resolve(
      filenamePolicySupplier = params.filenamePolicySupplier,
      prefix = params.prefix,
      shardNameTemplate = params.shardNameTemplate,
      isWindowed = isWindowed
    )(ScioUtil.strippedPath(path), params.suffix)

    val t = baseWrite
      .to(fp)
      .withTempDirectory(tempDirectory)
      .withNumShards(params.numShards)
      .withCodec(params.codec)
      .withMetadata(params.metadata.asJava)
      .withDatumWriterFactory(datumFactory)
      .pipe(w => if (!isWindowed) w else w.withWindowedWrites())

    data.applyInternal(t)
    tap(AvroIO.ReadParam(params))
  }
}
object AvroIO {

  @inline final def apply[T](path: String): TestIO[T] =
    new TestIO[T] {
      override val tapT: TapT.Aux[T, T] = TapOf[T]
      override def testId: String = s"AvroIO($path)"
    }
  object ReadParam {
    val DefaultSuffix: String = null
    private[scio] def apply(params: WriteParam): ReadParam =
      new ReadParam(params.suffix)
  }

  final case class ReadParam private (
    suffix: String = ReadParam.DefaultSuffix
  )

  object WriteParam {
    val DefaultNumShards: Int = 0
    val DefaultSuffix: String = ".avro"
    val DefaultSuffixObjectFile: String = ".obj.avro"
    val DefaultSuffixProtobuf: String = ".protobuf.avro"
    val DefaultCodec: CodecFactory = CodecFactory.deflateCodec(6)
    val DefaultMetadata: Map[String, AnyRef] = Map.empty
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultPrefix: String = null
    val DefaultShardNameTemplate: String = null
    val DefaultTempDirectory: String = null
  }

  final case class WriteParam private (
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    codec: CodecFactory = WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = WriteParam.DefaultMetadata,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )
}

final case class GenericRecordIO(
  path: String,
  schema: Schema,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends AvroIO[GenericRecord] {
  override protected def baseRead: BAvroIO.Read[GenericRecord] =
    BAvroIO.readGenericRecords(schema)
  override protected def baseWrite: BAvroIO.Write[GenericRecord] =
    BAvroIO.writeGenericRecords(schema)
  override def tap(read: ReadP): Tap[GenericRecord] =
    GenericRecordTap(path, schema, datumFactory, read)
}

object GenericRecordIO {
  type ReadParam = AvroIO.ReadParam
  val ReadParam = AvroIO.ReadParam
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}

final case class SpecificRecordIO[T <: SpecificRecord: ClassTag](
  path: String,
  datumFactory: AvroDatumFactory[T]
) extends AvroIO[T] {

  private val recordClass: Class[T] = ScioUtil.classOf[T]
  override def schema: Schema = SpecificData.get().getSchema(recordClass)
  override def baseRead: BAvroIO.Read[T] = BAvroIO.read(recordClass)
  override def baseWrite: BAvroIO.Write[T] = BAvroIO.write(recordClass)
  override def tap(read: ReadP): Tap[T] = SpecificRecordTap(path, datumFactory, read)
}

object SpecificRecordIO {
  private[scio] def defaultDatumFactory[T <: SpecificRecord: ClassTag]: AvroDatumFactory[T] =
    new SpecificRecordDatumFactory[T](ScioUtil.classOf[T])

  type ReadParam = AvroIO.ReadParam
  val ReadParam = AvroIO.ReadParam
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam

  def apply[T <: SpecificRecord: ClassTag](path: String): SpecificRecordIO[T] =
    SpecificRecordIO[T](path, defaultDatumFactory)
}

/**
 * Given a parseFn, read [[org.apache.avro.generic.GenericRecord GenericRecord]] and apply a
 * function mapping [[GenericRecord => T]] before producing output. This IO applies the function at
 * the time of de-serializing Avro GenericRecords.
 *
 * This IO doesn't define write, and should not be used to write Avro GenericRecords.
 */
final case class GenericRecordParseIO[T: Coder](path: String, parseFn: GenericRecord => T)
    extends ScioIO[T] {
  override type ReadP = GenericRecordParseIO.ReadParam
  override type WriteP = Nothing // Output is not defined for Avro Generic Parse IO.
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection[T] by applying the [[parseFn]] on
   * [[org.apache.avro.generic.GenericRecord GenericRecord]] from an Avro file.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val t = BAvroIO
      .parseGenericRecords(Functions.serializableFn(parseFn))
      .from(filePattern)
      .withCoder(coder)

    sc.applyTransform(t)
  }

  /** Writes are undefined for [[GenericRecordParseIO]] since it is used only for reading. */
  override protected def write(data: SCollection[T], params: Nothing): Tap[Nothing] = ???

  override def tap(read: ReadP): Tap[Nothing] = EmptyTap
}

object GenericRecordParseIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
}

final case class ObjectFileIO[T: Coder](
  path: String,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends ScioIO[T] {
  override type ReadP = ObjectFileIO.ReadParam
  override type WriteP = ObjectFileIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

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
    import underlying.coder
    val objectCoder = CoderMaterializer.beamWithDefault(Coder[T])
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

final case class ProtobufIO[T <: Message: ClassTag: Coder](
  path: String,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends ScioIO[T] {
  override type ReadP = ProtobufIO.ReadParam
  override type WriteP = ProtobufIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  private lazy val underlying: ObjectFileIO[T] = ObjectFileIO(path, datumFactory)

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params)

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val metadata = params.metadata ++ ProtobufUtil.schemaMetadataOf[T]
    data.write(underlying)(params.copy(metadata = metadata)).underlying
  }

  override def tap(read: ReadP): Tap[T] =
    ProtobufFileTap(path, read)
}

object ProtobufIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}

final case class AvroTypedIO[T <: HasAvroAnnotation: TypeTag: Coder](
  path: String,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends ScioIO[T] {
  override type ReadP = AvroTypedIO.ReadParam
  override type WriteP = AvroTypedIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"AvroIO($path)"

  private val avroT = AvroType[T]
  private lazy val underlying: GenericRecordIO = GenericRecordIO(path, avroT.schema, datumFactory)

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params).map(avroT.fromGenericRecord)

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    import underlying.coder
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
