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

package com.spotify.scio.io.dynamic.syntax

import com.google.protobuf.Message
import com.spotify.scio.io.{ClosedTap, EmptyTap, TextIO}
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.util.{Functions, ProtobufUtil, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{Compression, FileIO}
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.extensions.avro.io.{AvroIO => BAvroIO}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.chaining._
import java.util.{HashMap => JHashMap}

object DynamicSCollectionOps {
  private[scio] def writeDynamic[A](
    path: String,
    destinationFn: A => String,
    numShards: Int,
    prefix: String,
    suffix: String,
    tempDirectory: String
  ): FileIO.Write[String, A] = {
    val naming = ScioUtil.defaultNaming(Option(prefix).getOrElse("part"), suffix) _
    FileIO
      .writeDynamic[String, A]()
      .to(path)
      .by(Functions.serializableFn(destinationFn))
      .withNumShards(numShards)
      .withDestinationCoder(StringUtf8Coder.of())
      .withNaming(Functions.serializableFn(naming))
      .pipe(t => Option(tempDirectory).fold(t)(t.withTempDirectory))
  }
}

final class DynamicSpecificRecordSCollectionOps[T <: SpecificRecord](
  private val self: SCollection[T]
) extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  /** Save this SCollection as Avro files specified by the destination function. */
  def saveAsDynamicAvroFile(
    path: String,
    numShards: Int = 0,
    suffix: String = ".avro",
    codec: CodecFactory = CodecFactory.deflateCodec(6),
    metadata: Map[String, AnyRef] = Map.empty,
    tempDirectory: String = null,
    prefix: String = null
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
    numShards: Int = 0,
    suffix: String = ".avro",
    codec: CodecFactory = CodecFactory.deflateCodec(6),
    metadata: Map[String, AnyRef] = Map.empty,
    tempDirectory: String = null,
    prefix: String = null
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

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  /** Save this SCollection as text files specified by the destination function. */
  def saveAsDynamicTextFile(
    path: String,
    numShards: Int = TextIO.WriteParam.DefaultNumShards,
    suffix: String = TextIO.WriteParam.DefaultSuffix,
    compression: Compression = TextIO.WriteParam.DefaultCompression,
    tempDirectory: String = TextIO.WriteParam.DefaultTempDirectory,
    prefix: String = TextIO.WriteParam.DefaultPrefix,
    header: Option[String] = TextIO.WriteParam.DefaultHeader,
    footer: Option[String] = TextIO.WriteParam.DefaultFooter
  )(destinationFn: String => String)(implicit ct: ClassTag[T]): ClosedTap[Nothing] = {
    val s = if (classOf[String] isAssignableFrom ct.runtimeClass) {
      self.asInstanceOf[SCollection[String]]
    } else {
      self.map(_.toString)
    }
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Text file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = beam.TextIO
        .sink()
        .pipe(s => header.fold(s)(s.withHeader))
        .pipe(s => footer.fold(s)(s.withFooter))

      val write = writeDynamic(
        path = path,
        destinationFn = destinationFn,
        numShards = numShards,
        prefix = prefix,
        suffix = suffix,
        tempDirectory = tempDirectory
      ).via(sink)
        .withCompression(compression)
      s.applyInternal(write)
    }

    ClosedTap[Nothing](EmptyTap)
  }
}

final class DynamicProtobufSCollectionOps[T <: Message](private val self: SCollection[T])
    extends AnyVal {
  import DynamicSCollectionOps.writeDynamic

  def saveAsDynamicProtobufFile(
    path: String,
    numShards: Int = 0,
    suffix: String = ".protobuf",
    codec: CodecFactory = CodecFactory.deflateCodec(6),
    metadata: Map[String, AnyRef] = Map.empty,
    tempDirectory: String = null,
    prefix: String = null
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

trait SCollectionSyntax {
  implicit def dynamicSpecificRecordSCollectionOps[T <: SpecificRecord](
    sc: SCollection[T]
  ): DynamicSpecificRecordSCollectionOps[T] =
    new DynamicSpecificRecordSCollectionOps(sc)

  implicit def dynamicGenericRecordSCollectionOps(
    sc: SCollection[GenericRecord]
  ): DynamicGenericRecordSCollectionOps =
    new DynamicGenericRecordSCollectionOps(sc)

  implicit def dynamicSCollectionOps[T](sc: SCollection[T]): DynamicSCollectionOps[T] =
    new DynamicSCollectionOps(sc)

  implicit def dynamicProtobufSCollectionOps[T <: Message](
    sc: SCollection[T]
  ): DynamicProtobufSCollectionOps[T] = new DynamicProtobufSCollectionOps(sc)
}
