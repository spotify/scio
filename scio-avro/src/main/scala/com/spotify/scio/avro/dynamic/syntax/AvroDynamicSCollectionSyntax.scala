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

package com.spotify.scio.avro.dynamic.syntax

import com.google.protobuf.Message
import com.spotify.scio.avro.AvroIO
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps
import com.spotify.scio.protobuf.util.ProtobufUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.extensions.avro.io.{AvroIO => BAvroIO}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import java.util.{HashMap => JHashMap}

final class DynamicSpecificRecordSCollectionOps[T <: SpecificRecord](
  private val self: SCollection[T]
) extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  /** Save this SCollection as Avro files specified by the destination function. */
  def saveAsDynamicAvroFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = AvroIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(
    destinationFn: T => String
  )(implicit ct: ClassTag[T]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Avro file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val cls = ct.runtimeClass.asInstanceOf[Class[T]]
      val nm = new JHashMap[String, AnyRef]()
      nm.putAll(metadata.asJava)
      val sink = BAvroIO
        .sink(cls)
        .withCodec(codec)
        .withMetadata(nm)
      val write =
        writeDynamic(
          path = path,
          destinationFn = destinationFn,
          numShards = numShards,
          prefix = prefix,
          suffix = suffix,
          tempDirectory = tempDirectory
        ).via(sink)

      self.applyInternal(write)
    }

    ClosedTap[Nothing](EmptyTap)
  }
}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicGenericRecordSCollectionOps(private val self: SCollection[GenericRecord])
    extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  /** Save this SCollection as Avro files specified by the destination function. */
  def saveAsDynamicAvroFile(
    path: String,
    schema: Schema,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = AvroIO.WriteParam.DefaultSuffix,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(
    destinationFn: GenericRecord => String
  ): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Avro file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val nm = new JHashMap[String, AnyRef]()
      nm.putAll(metadata.asJava)
      val sink = BAvroIO
        .sink[GenericRecord](schema)
        .withCodec(codec)
        .withMetadata(nm)
      val write =
        writeDynamic(
          path = path,
          destinationFn = destinationFn,
          numShards = numShards,
          prefix = prefix,
          suffix = suffix,
          tempDirectory = tempDirectory
        ).via(sink)

      self.applyInternal(write)
    }

    ClosedTap[Nothing](EmptyTap)
  }
}

final class DynamicProtobufSCollectionOps[T <: Message](private val self: SCollection[T])
    extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  def saveAsDynamicProtobufFile(
    path: String,
    numShards: Int = AvroIO.WriteParam.DefaultNumShards,
    suffix: String = AvroIO.WriteParam.DefaultSuffixProtobuf,
    codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
    metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata,
    tempDirectory: String = AvroIO.WriteParam.DefaultTempDirectory,
    prefix: String = AvroIO.WriteParam.DefaultPrefix
  )(destinationFn: T => String)(implicit ct: ClassTag[T]): ClosedTap[Nothing] = {
    val protoCoder = Coder.protoMessageCoder[T]
    val elemCoder = CoderMaterializer.beam(self.context, protoCoder)
    val avroSchema = AvroBytesUtil.schema
    val nm = new JHashMap[String, AnyRef]()
    nm.putAll((metadata ++ ProtobufUtil.schemaMetadataOf(ct)).asJava)

    if (self.context.isTest) {
      throw new NotImplementedError(
        "Protobuf file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = BAvroIO
        .sinkViaGenericRecords(
          avroSchema,
          (element: T, _: Schema) => AvroBytesUtil.encode(elemCoder, element)
        )
        .withCodec(codec)
        .withMetadata(nm)
      val write = writeDynamic(
        path = path,
        destinationFn = destinationFn,
        numShards = numShards,
        prefix = prefix,
        suffix = suffix,
        tempDirectory = tempDirectory
      ).via(sink)

      self.applyInternal(write)
    }

    ClosedTap[Nothing](EmptyTap)
  }
}

trait AvroDynamicSCollectionSyntax {
  implicit def dynamicSpecificRecordSCollectionOps[T <: SpecificRecord](
    sc: SCollection[T]
  ): DynamicSpecificRecordSCollectionOps[T] =
    new DynamicSpecificRecordSCollectionOps(sc)

  implicit def dynamicGenericRecordSCollectionOps(
    sc: SCollection[GenericRecord]
  ): DynamicGenericRecordSCollectionOps =
    new DynamicGenericRecordSCollectionOps(sc)

  implicit def dynamicProtobufSCollectionOps[T <: Message](
    sc: SCollection[T]
  ): DynamicProtobufSCollectionOps[T] = new DynamicProtobufSCollectionOps(sc)
}
