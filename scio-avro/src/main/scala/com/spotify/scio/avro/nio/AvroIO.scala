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

package com.spotify.scio.avro.nio

import com.google.protobuf.Message
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.{io => gio}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.specific.SpecificRecordBase
import com.spotify.scio.ScioContext
import com.spotify.scio.values._
import com.spotify.scio.io.Tap
import com.spotify.scio.avro.io._
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.Implicits._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class ObjectFile[T: ClassTag](path: String)
  extends ScioIO[T] {

  type ReadP = Unit
  type WriteP = ObjectFile.WriteParam

  def id: String = path

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group input
   */
  def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.requireNotClosed {
      val coder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
      sc.read(AvroFile[GenericRecord](path, AvroBytesUtil.schema))
        .parDo(new DoFn[GenericRecord, T] {
          @ProcessElement
          private[scio] def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit = {
            c.output(AvroBytesUtil.decode(coder, c.element()))
          }
        })
        .setName(path)
    }

  def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group output
   */
  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] =
    params match {
      case ObjectFile.Parameters(numShards, suffix, metadata) =>
        val elemCoder = sc.getCoder[T]
        val parameters = AvroFile.Parameters(numShards, suffix, metadata = metadata)
        sc
          .parDo(new DoFn[T, GenericRecord] {
            @ProcessElement
            private[scio] def processElement(c: DoFn[T, GenericRecord]#ProcessContext): Unit =
              c.output(AvroBytesUtil.encode(elemCoder, c.element()))
          })
          .write(AvroFile[GenericRecord](path, AvroBytesUtil.schema))(parameters)
        sc.context.makeFuture(ObjectFileTap[T](ScioUtil.addPartSuffix(path)))
    }
}

object ObjectFile {
  sealed trait WriteParam
  final case class Parameters(
    numShards: Int = 0,
    suffix: String = ".obj",
    metadata: Map[String, AnyRef] = Map.empty) extends WriteParam
}

case class ProtobufFile[T : ClassTag](path: String)(implicit ev: T <:< Message) extends ScioIO[T] {
  type ReadP = Unit
  type WriteP = ProtobufFile.WriteParam

  def id: String = path

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group input
   */
  def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(ObjectFile(path))

  def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](path)

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group output
   */
  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] =
    params match {
      case ProtobufFile.Parameters(numShards) =>
        import me.lyh.protobuf.generic
        val schema = generic.Schema.of[Message](sc.ct.asInstanceOf[ClassTag[Message]]).toJson
        val metadata = Map("protobuf.generic.schema" -> schema)
        sc.write(ObjectFile[T](path))(ObjectFile.Parameters(numShards))
    }
}

object ProtobufFile {
  sealed trait WriteParam
  final case class Parameters(numShards: Int = 0) extends WriteParam
}

case class AvroFile[T: ClassTag](path: String, schema: Schema = null)
  extends ScioIO[T] {

  type ReadP = Unit
  type WriteP = AvroFile.WriteParam

  private def avroOut[U](sc: SCollection[T],
                         write: gio.AvroIO.Write[U],
                         path: String, numShards: Int, suffix: String,
                         codec: CodecFactory,
                         metadata: Map[String, AnyRef]) =
    write
      .to(sc.pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix + ".avro")
      .withCodec(codec)
      .withMetadata(metadata.asJava)

  def id: String = path

  /**
   * Get an SCollection for an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group input
   */
  def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.requireNotClosed {
      val cls = ScioUtil.classOf[T]
      val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        gio.AvroIO.read(cls).from(path)
      } else {
        gio.AvroIO.readGenericRecords(schema).from(path).asInstanceOf[gio.AvroIO.Read[T]]
      }
      sc.wrap(sc.applyInternal(t)).setName(path)
    }

  def tap(read: ReadP): Tap[T] =
    AvroTap[T](path, schema)

  /**
   * Save this SCollection as an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group output
   */
  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] =
    params match {
      case AvroFile.Parameters(numShards, suffix, codec, metadata) =>
        val cls = ScioUtil.classOf[T]
        val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
          gio.AvroIO.write(cls)
        } else {
          gio.AvroIO.writeGenericRecords(schema).asInstanceOf[gio.AvroIO.Write[T]]
        }
        sc.applyInternal(avroOut(sc, t, path, numShards, suffix, codec, metadata))
        sc.context.makeFuture(AvroTap(ScioUtil.addPartSuffix(path), schema))
    }
}

object AvroFile {
  sealed trait WriteParam
  final case class Parameters(
    numShards: Int = 0,
    suffix: String = "",
    codec: CodecFactory = CodecFactory.deflateCodec(6),
    metadata: Map[String, AnyRef] = Map.empty) extends WriteParam
}

object Typed {

  case class AvroFile[T : ClassTag : TypeTag](path: String)(implicit ev: T <:< HasAvroAnnotation)
    extends ScioIO[T] {

    type ReadP = Unit
    type WriteP = AvroFile.WriteParam

    private def typedAvroOut[U](
                              sc: SCollection[T],
                              write: gio.AvroIO.TypedWrite[U, Void, GenericRecord],
                              path: String, numShards: Int, suffix: String,
                              codec: CodecFactory,
                              metadata: Map[String, AnyRef]) =
    write
      .to(sc.pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix + ".avro")
      .withCodec(codec)
      .withMetadata(metadata.asJava)

    def id: String = path

  /**
   * Get a typed SCollection from an Avro schema.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
   * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   *
   * @group input
   */
    def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.requireNotClosed {
        val avroT = AvroType[T]
        val t = gio.AvroIO.readGenericRecords(avroT.schema).from(path)
        sc.wrap(sc.applyInternal(t)).setName(path).map(avroT.fromGenericRecord)
      }


    def tap(read: ReadP): Tap[T] = {
      val avroT = AvroType[T]
      AvroTap[GenericRecord](path, avroT.schema)
        .map(avroT.fromGenericRecord)
    }

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   * @group output
   */
    def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] =
      params match {
        case AvroFile.Parameters(numShards, suffix, codec, metadata) =>
          val avroT = AvroType[T]
          val t = gio.AvroIO.writeCustomTypeToGenericRecords()
            .withFormatFunction(new SerializableFunction[T, GenericRecord] {
              override def apply(input: T): GenericRecord = avroT.toGenericRecord(input)
            })
            .withSchema(avroT.schema)
          sc.applyInternal(typedAvroOut(sc, t, path, numShards, suffix, codec, metadata))
          sc.context.makeFuture(AvroTap(ScioUtil.addPartSuffix(path), avroT.schema))
      }
  }

  object AvroFile {
    sealed trait WriteParam
    final case class Parameters(
      numShards: Int = 0,
      suffix: String = "",
      codec: CodecFactory = CodecFactory.deflateCodec(6),
      metadata: Map[String, AnyRef] = Map.empty) extends WriteParam
  }

}
