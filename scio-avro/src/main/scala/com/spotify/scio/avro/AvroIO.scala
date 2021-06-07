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
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.util.{Functions, ProtobufUtil, ScioUtil}
import com.spotify.scio.values._
import com.spotify.scio.{avro, ScioContext}
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}
import org.apache.beam.sdk.{io => beam}

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

final case class ProtobufIO[T <: Message: ClassTag](path: String) extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = ProtobufIO.WriteParam
  final override val tapT: TapT.Aux[T, T] = TapOf[T]
  private val protoCoder = Coder.protoMessageCoder[T]

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(ObjectFileIO[T](path)(protoCoder))

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val metadata = params.metadata ++ ProtobufUtil.schemaMetadataOf[T]
    data.write(ObjectFileIO[T](path)(protoCoder))(params.copy(metadata = metadata)).underlying
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))(protoCoder)
}

object ProtobufIO {
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}

sealed trait AvroIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, T] = TapOf[T]

  protected def avroOut[U](
    write: beam.AvroIO.Write[U],
    path: String,
    numShards: Int,
    suffix: String,
    codec: CodecFactory,
    metadata: Map[String, AnyRef]
  ) =
    write
      .to(ScioUtil.pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix)
      .withCodec(codec)
      .withMetadata(metadata.asJava)
}

final case class SpecificRecordIO[T <: SpecificRecord: ClassTag: Coder](path: String)
    extends AvroIO[T] {
  override type ReadP = Unit
  override type WriteP = AvroIO.WriteParam

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection of [[org.apache.avro.specific.SpecificRecord SpecificRecord]]
   * from an Avro file.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val cls = ScioUtil.classOf[T]
    val t = beam.AvroIO.read(cls).from(path)
    sc.applyTransform(t)
  }

  /**
   * Save this SCollection of [[org.apache.avro.specific.SpecificRecord SpecificRecord]] as
   * an Avro file.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val cls = ScioUtil.classOf[T]
    val t = beam.AvroIO.write(cls)
    data.applyInternal(
      avroOut(t, path, params.numShards, params.suffix, params.codec, params.metadata)
    )
    tap(())
  }

  override def tap(read: ReadP): Tap[T] =
    SpecificRecordTap[T](ScioUtil.addPartSuffix(path))
}

final case class GenericRecordIO(path: String, schema: Schema) extends AvroIO[GenericRecord] {
  override type ReadP = Unit
  override type WriteP = AvroIO.WriteParam

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection of [[org.apache.avro.generic.GenericRecord GenericRecord]] from an Avro
   * file.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[GenericRecord] = {
    val t = beam.AvroIO
      .readGenericRecords(schema)
      .from(path)
    sc.applyTransform(t)
  }

  /** Save this SCollection [[org.apache.avro.generic.GenericRecord GenericRecord]] as a Avro file. */
  override protected def write(
    data: SCollection[GenericRecord],
    params: WriteP
  ): Tap[GenericRecord] = {
    val t = beam.AvroIO.writeGenericRecords(schema)
    data.applyInternal(
      avroOut(t, path, params.numShards, params.suffix, params.codec, params.metadata)
    )
    tap(())
  }

  override def tap(read: ReadP): Tap[GenericRecord] =
    GenericRecordTap(ScioUtil.addPartSuffix(path), schema)
}

/**
 * Given a parseFn, read [[org.apache.avro.generic.GenericRecord GenericRecord]]
 * and apply a function mapping [[GenericRecord => T]] before producing output.
 * This IO applies the function at the time of de-serializing Avro GenericRecords.
 *
 * This IO doesn't define write, and should not be used to write Avro GenericRecords.
 */
final case class GenericRecordParseIO[T](path: String, parseFn: GenericRecord => T)(implicit
  coder: Coder[T]
) extends AvroIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing // Output is not defined for Avro Generic Parse IO.

  override def testId: String = s"AvroIO($path)"

  /**
   * Get an SCollection[T] by applying the [[parseFn]] on
   * [[org.apache.avro.generic.GenericRecord GenericRecord]]
   * from an Avro file.
   */
  override protected def read(sc: ScioContext, params: Unit): SCollection[T] = {
    val t = beam.AvroIO
      .parseGenericRecords(Functions.serializableFn(parseFn))
      .from(path)
      .withCoder(CoderMaterializer.beam(sc, coder))

    sc.applyTransform(t)
  }

  /** Writes are undefined for [[GenericRecordParseIO]] since it is used only for reading. */
  override protected def write(data: SCollection[T], params: Nothing): Tap[T] = ???

  override def tap(read: Unit): Tap[T] =
    GenericRecordParseTap[T](ScioUtil.addPartSuffix(path), parseFn)
}

object AvroIO {
  object WriteParam {
    private[avro] val DefaultNumShards = 0
    private[avro] val DefaultSuffix = ""
    private[avro] val DefaultCodec: CodecFactory = CodecFactory.deflateCodec(6)
    private[avro] val DefaultMetadata: Map[String, AnyRef] = Map.empty
  }

  final case class WriteParam private[avro] (
    numShards: Int = WriteParam.DefaultNumShards,
    private val _suffix: String = WriteParam.DefaultSuffix,
    codec: CodecFactory = WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = WriteParam.DefaultMetadata
  ) {
    val suffix: String = _suffix + ".avro"
  }

  @inline final def apply[T](id: String): AvroIO[T] =
    new AvroIO[T] with TestIO[T] {
      override def testId: String = s"AvroIO($id)"
    }
}

object AvroTyped {
  final case class AvroIO[T <: HasAvroAnnotation: TypeTag: Coder](path: String) extends ScioIO[T] {
    override type ReadP = Unit
    override type WriteP = avro.AvroIO.WriteParam
    final override val tapT: TapT.Aux[T, T] = TapOf[T]

    private def typedAvroOut[U](
      write: beam.AvroIO.TypedWrite[U, Void, GenericRecord],
      path: String,
      numShards: Int,
      suffix: String,
      codec: CodecFactory,
      metadata: Map[String, AnyRef]
    ) =
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
    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      val avroT = AvroType[T]
      val t = beam.AvroIO.readGenericRecords(avroT.schema).from(path)
      sc.applyTransform(t).map(avroT.fromGenericRecord)
    }

    /**
     * Save this SCollection as an Avro file. Note that element type `T` must be a case class
     * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
     */
    override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
      val avroT = AvroType[T]
      val t = beam.AvroIO
        .writeCustomTypeToGenericRecords()
        .withFormatFunction(new SerializableFunction[T, GenericRecord] {
          override def apply(input: T): GenericRecord =
            avroT.toGenericRecord(input)
        })
        .withSchema(avroT.schema)
      data.applyInternal(
        typedAvroOut(t, path, params.numShards, params.suffix, params.codec, params.metadata)
      )
      tap(())
    }

    override def tap(read: ReadP): Tap[T] = {
      val avroT = AvroType[T]
      GenericRecordTap(ScioUtil.addPartSuffix(path), avroT.schema)
        .map(avroT.fromGenericRecord)
    }
  }
}
