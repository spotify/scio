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

import com.google.protobuf.Message
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values._
import com.spotify.scio.{avro, ScioContext}
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}
import org.apache.beam.sdk.{io => beam}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.runtime.universe._
import scala.reflect.{classTag, ClassTag}

final case class ObjectFileIO[T: Coder](path: String) extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = ObjectFileIO.WriteParam
  override final val tapT = TapOf[T]

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    implicit val bcoder = Coder.avroGenericRecordCoder(AvroBytesUtil.schema)
    AvroIO[GenericRecord](path, AvroBytesUtil.schema)
      .read(sc, params)
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
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val elemCoder = CoderMaterializer.beam(data.context, Coder[T])
    implicit val bcoder = Coder.avroGenericRecordCoder(AvroBytesUtil.schema)
    val bytes = data
      .parDo(new DoFn[T, GenericRecord] {
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, GenericRecord]#ProcessContext): Unit =
          c.output(AvroBytesUtil.encode(elemCoder, c.element()))
      })
    AvroIO[GenericRecord](path, AvroBytesUtil.schema).write(bytes, params)
    data.context.makeFuture(tap(Unit))
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))
}

object ObjectFileIO {
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}

final case class ProtobufIO[T: ClassTag](path: String)(implicit ev: T <:< Message)
    extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = ProtobufIO.WriteParam
  override final val tapT = TapOf[T]

  private val protoCoder =
    Coder
      .protoMessageCoder[Message](classTag[T].asInstanceOf[ClassTag[Message]])
      .asInstanceOf[Coder[T]]

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    ObjectFileIO[T](path)(protoCoder).read(sc, params)

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    import me.lyh.protobuf.generic
    val schema = generic.Schema
      .of[Message](classTag[T].asInstanceOf[ClassTag[Message]])
      .toJson
    val metadata = params.metadata ++ Map("protobuf.generic.schema" -> schema)
    ObjectFileIO[T](path)(protoCoder).write(data, params.copy(metadata = metadata))
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))(protoCoder)
}

object ProtobufIO {
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}
sealed trait AvroIO[T] extends ScioIO[T] {
  override final val tapT = TapOf[T]

  protected def avroOut[U](sc: SCollection[T],
                           write: beam.AvroIO.Write[U],
                           path: String,
                           numShards: Int,
                           suffix: String,
                           codec: CodecFactory,
                           metadata: Map[String, AnyRef]) =
    write
      .to(ScioUtil.pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix)
      .withCodec(codec)
      .withMetadata(metadata.asJava)

}

final case class SpecificRecordIO[T: ClassTag: Coder](path: String)(
  implicit ev: T <:< SpecificRecordBase)
    extends AvroIO[T] {
  override type ReadP = Unit
  override type WriteP = AvroIO.WriteParam

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection for an Avro file.  It will return `T` of type
   * [[org.apache.avro.specific.SpecificRecordBase SpecificRecordBase]].
   */
  override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val cls = ScioUtil.classOf[T]
    val t = beam.AvroIO.read(cls).from(path)
    sc.wrap(sc.applyInternal(t))
  }

  /**
   * Save this SCollection as an Avro file. `schema` must be not null if `T` is of type
   * [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val cls = ScioUtil.classOf[T]
    val t = beam.AvroIO.write(cls)
    data.applyInternal(
      avroOut(data, t, path, params.numShards, params.suffix, params.codec, params.metadata))
    data.context.makeFuture(tap(Unit))
  }

  override def tap(read: ReadP): Tap[T] =
    SpecificRecordAvroTap[T](ScioUtil.addPartSuffix(path))
}

final case class SchemaAvroIO[T: Coder](path: String, schema: Schema)(
  implicit ev: T <:< GenericRecord)
    extends AvroIO[T] {
  override type ReadP = Unit
  override type WriteP = AvroIO.WriteParam

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection for an Avro file. `schema` must be not null if `T` is of type
   * [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val t = beam.AvroIO
      .readGenericRecords(schema)
      .from(path)
      .asInstanceOf[beam.AvroIO.Read[T]]
    sc.wrap(sc.applyInternal(t))
  }

  /**
   * Save this SCollection as an Avro file. `schema` must be not null if `T` is of type
   * [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val t = beam.AvroIO.writeGenericRecords(schema).asInstanceOf[beam.AvroIO.Write[T]]
    data.applyInternal(
      avroOut(data, t, path, params.numShards, params.suffix, params.codec, params.metadata))
    data.context.makeFuture(tap(Unit))
  }

  override def tap(read: ReadP): Tap[T] =
    SchemaAvroTap[T](ScioUtil.addPartSuffix(path), schema)
}

object AvroIO {
  object WriteParam {
    private[avro] val DefaultNumShards = 0
    private[avro] val DefaultSuffix = ""
    private[avro] val DefaultCodec: CodecFactory = CodecFactory.deflateCodec(6)
    private[avro] val DefaultMetadata: Map[String, AnyRef] = Map.empty
  }

  final case class WriteParam private (numShards: Int = WriteParam.DefaultNumShards,
                                       private val _suffix: String = WriteParam.DefaultSuffix,
                                       codec: CodecFactory = WriteParam.DefaultCodec,
                                       metadata: Map[String, AnyRef] = WriteParam.DefaultMetadata) {
    val suffix: String = _suffix + ".avro"
  }

  def apply[T: ClassTag: Coder](path: String)(
    implicit ev: T <:< SpecificRecordBase): SpecificRecordIO[T] =
    SpecificRecordIO[T](path)

  def apply[T: Coder](path: String, schema: Schema)(
    implicit ev: T <:< GenericRecord): SchemaAvroIO[T] =
    SchemaAvroIO[T](path, schema)
}

object AvroTyped {

  final case class AvroIO[T: ClassTag: TypeTag: Coder](path: String)(
    implicit ev: T <:< HasAvroAnnotation)
      extends ScioIO[T] {

    override type ReadP = Unit
    override type WriteP = avro.AvroIO.WriteParam
    override final val tapT = TapOf[T]

    private def typedAvroOut[U](sc: SCollection[T],
                                write: beam.AvroIO.TypedWrite[U, Void, GenericRecord],
                                path: String,
                                numShards: Int,
                                suffix: String,
                                codec: CodecFactory,
                                metadata: Map[String, AnyRef]) =
      write
        .to(ScioUtil.pathWithShards(path))
        .withNumShards(numShards)
        .withSuffix(suffix)
        .withCodec(codec)
        .withMetadata(metadata.asJava)

    /**
     * Get a typed SCollection from an Avro schema.
     *
     * Note that `T` must be annotated with
     * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
     * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
     * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
     */
    override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      val avroT = AvroType[T]
      val t = beam.AvroIO.readGenericRecords(avroT.schema).from(path)
      sc.wrap(sc.applyInternal(t)).map(avroT.fromGenericRecord)
    }

    /**
     * Save this SCollection as an Avro file. Note that element type `T` must be a case class
     * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
     */
    override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
      val avroT = AvroType[T]
      val t = beam.AvroIO
        .writeCustomTypeToGenericRecords()
        .withFormatFunction(new SerializableFunction[T, GenericRecord] {
          override def apply(input: T): GenericRecord =
            avroT.toGenericRecord(input)
        })
        .withSchema(avroT.schema)
      data.applyInternal(
        typedAvroOut(data, t, path, params.numShards, params.suffix, params.codec, params.metadata))
      data.context.makeFuture(tap(Unit))
    }

    override def tap(read: ReadP): Tap[T] = {
      val avroT = AvroType[T]
      implicit val bcoder = Coder.avroGenericRecordCoder(avroT.schema)
      SchemaAvroTap[GenericRecord](ScioUtil.addPartSuffix(path), avroT.schema)
        .map(avroT.fromGenericRecord)
    }
  }

}
