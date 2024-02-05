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

package com.spotify.scio.parquet.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.parquet.avro.ParquetAvroIO.ReadParam.{
  DefaultConfiguration,
  DefaultPredicate,
  DefaultProjection,
  DefaultSuffix
}
import com.spotify.scio.parquet.avro.ParquetAvroIO.WriteParam._
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.parquet.{GcsConnectorUtil, ParquetConfiguration}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{FilenamePolicySupplier, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord, SpecificRecordBase}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.{PTransform, SerializableFunctions, SimpleFunction}
import org.apache.beam.sdk.values.{PBegin, PCollection, TypeDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

sealed trait ParquetAvroIO[T <: IndexedRecord] extends ScioIO[T] {

  override type ReadP = ParquetAvroIO.ReadParam[T]
  override type WriteP = ParquetAvroIO.WriteParam[T]
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"ParquetAvroIO($path)"

  def path: String
  protected def schema: Schema
  protected def defaultDatumFactory: AvroDatumFactory[T]

  protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val conf = ParquetConfiguration.ofNullable(params.conf)
    if (ParquetReadConfiguration.getUseSplittableDoFn(conf, sc.options)) {
      readSplittableDoFn(sc, conf, params)
    } else {
      readLegacy(sc, conf, params)
    }
  }

  private def readSplittableDoFn(
    sc: ScioContext,
    conf: Configuration,
    params: ReadP
  ): SCollection[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val datumFactory = Option(params.datumFactory).getOrElse(defaultDatumFactory)
    val coder = avroCoder(datumFactory, schema)
    val bCoder = CoderMaterializer.beam(sc, coder)
    val transform = new PTransform[PBegin, PCollection[T]] {
      override def expand(input: PBegin): PCollection[T] = {
        input
          .apply(FileIO.`match`().filepattern(filePattern))
          .apply(FileIO.readMatches)
          .apply(readFiles(params.projection, params.predicate, conf))
      }
    }
    sc.applyTransform(transform).setCoder(bCoder)
  }

  private def readLegacy(
    sc: ScioContext,
    conf: Configuration,
    params: ReadP
  ): SCollection[T] = {
    conf.setClass("key.class", classOf[Void], classOf[Void])
    implicit val keyCoder: Coder[Void] = Coder.voidCoder
    val bKeyCoder = CoderMaterializer.beam(sc, keyCoder)

    val datumFactory = Option(params.datumFactory).getOrElse(defaultDatumFactory)
    val recordClass = datumFactory.getType
    conf.setClass("value.class", recordClass, recordClass)
    implicit val valueCoder: Coder[T] = avroCoder(datumFactory, schema)
    val bValueCoder = CoderMaterializer.beam(sc, valueCoder)

    AvroReadSupport.setAvroReadSchema(conf, schema)
    if (recordClass == classOf[GenericRecord]) {
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    }

    Option(params.projection).foreach(p => AvroReadSupport.setRequestedProjection(conf, p))
    Option(params.predicate).foreach(p => ParquetInputFormat.setFilterPredicate(conf, p))

    val job = Job.getInstance(conf)
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    GcsConnectorUtil.setInputPaths(sc, job, filePattern)
    job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
    val transform = HadoopFormatIO
      .read[Void, T]()
      // Force coders for hadoop job
      .withKeyTranslation(ParquetAvroIO.Identity[Void], bKeyCoder)
      .withValueTranslation(ParquetAvroIO.Identity(recordClass), bValueCoder)
      .withConfiguration(job.getConfiguration)

    sc.applyTransform(transform).map(_.getValue)
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[T] = {
    val datumFactory = Option(params.datumFactory).getOrElse(defaultDatumFactory)
    implicit val coder: Coder[T] = avroCoder(datumFactory, schema)
    // SpecificData.getForClass is only available for 1.9+
    val recordClass = datumFactory.getType
    val data = if (classOf[SpecificRecordBase].isAssignableFrom(recordClass)) {
      val classModelField = recordClass.getDeclaredField("MODEL$")
      classModelField.setAccessible(true)
      classModelField.get(null).asInstanceOf[SpecificData]
    } else {
      SpecificData.get()
    }
    // The projection function is not part of the test input, so it must be applied directly
    val projectedFields = Option(params.projection).map(_.getFields.asScala.map(_.name()).toSet)
    TestDataManager
      .getInput(sc.testId.get)(this)
      .toSCollection(sc)
      .map { record =>
        projectedFields match {
          case None             => record
          case Some(projection) =>
            // beam forbids mutations. Create a new record
            val copy = data.deepCopy(record.getSchema, record)
            record.getSchema.getFields.asScala
              .foldLeft(copy) { (c, f) =>
                val names = Set(f.name()) ++ f.aliases().asScala.toSet
                if (projection.intersect(names).isEmpty) {
                  // field is not part of the projection. user default value
                  c.put(f.pos(), data.getDefaultValue(f))
                }
                c
              }
        }
      }
  }

  private def parquetOut(
    path: String,
    schema: Schema,
    suffix: String,
    numShards: Int,
    compression: CompressionCodecName,
    conf: Configuration,
    filenamePolicySupplier: FilenamePolicySupplier,
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean,
    tempDirectory: ResourceId,
    isLocalRunner: Boolean
  ) = {
    require(tempDirectory != null, "tempDirectory must not be null")
    val fp = FilenamePolicySupplier.resolve(
      filenamePolicySupplier = filenamePolicySupplier,
      prefix = prefix,
      shardNameTemplate = shardNameTemplate,
      isWindowed = isWindowed
    )(ScioUtil.strippedPath(path), suffix)
    val dynamicDestinations = DynamicFileDestinations
      .constant(fp, SerializableFunctions.identity[T])
    val job = Job.getInstance(conf)
    if (isLocalRunner) GcsConnectorUtil.setCredentials(job)

    val sink = new ParquetAvroFileBasedSink[T](
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      schema,
      job.getConfiguration,
      compression
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val conf = ParquetConfiguration.ofNullable(params.conf)
    data.applyInternal(
      parquetOut(
        path,
        schema,
        params.suffix,
        params.numShards,
        params.compression,
        conf,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        ScioUtil.isLocalRunner(data.context.options.getRunner)
      )
    )
    tap(ParquetAvroIO.ReadParam(params))
  }
}

object ParquetAvroIO {

  private class Identity[T](cls: Class[T])
      extends SimpleFunction[T, T](SerializableFunctions.identity[T]) {
    override def getInputTypeDescriptor: TypeDescriptor[T] = TypeDescriptor.of(cls)
    override def getOutputTypeDescriptor: TypeDescriptor[T] = TypeDescriptor.of(cls)
  }

  private object Identity {
    def apply[T: ClassTag]: Identity[T] = new Identity(ScioUtil.classOf[T])
    def apply[T](cls: Class[T]): Identity[T] = new Identity(cls)
  }

  @inline final def apply[T](path: String): TestIO[T] =
    new TestIO[T] {
      override val tapT: TapT.Aux[T, T] = TapOf[T]
      override def testId: String = s"ParquetAvroIO($path)"
    }

  object ReadParam {
    val DefaultProjection: Schema = null
    val DefaultPredicate: FilterPredicate = null
    val DefaultConfiguration: Configuration = null
    val DefaultSuffix: String = null
    val DefaultDatumFactory: Null = null

    private[scio] def apply[T](params: WriteParam[T]): ReadParam[T] =
      new ReadParam(
        projection = DefaultProjection,
        predicate = DefaultPredicate,
        conf = params.conf,
        suffix = params.suffix,
        datumFactory = params.datumFactory
      )
  }

  final case class ReadParam[T] private (
    projection: Schema = DefaultProjection,
    predicate: FilterPredicate = DefaultPredicate,
    conf: Configuration = DefaultConfiguration,
    suffix: String = DefaultSuffix,
    datumFactory: AvroDatumFactory[T] = DefaultDatumFactory
  )

  object WriteParam {
    val DefaultNumShards: Int = 0
    val DefaultSuffix: String = ".parquet"
    val DefaultCompression: CompressionCodecName = CompressionCodecName.ZSTD
    val DefaultConfiguration: Configuration = null
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultPrefix: String = null
    val DefaultShardNameTemplate: String = null
    val DefaultTempDirectory: String = null
    val DefaultDatumFactory: Null = null
  }

  final case class WriteParam[T] private (
    numShards: Int = DefaultNumShards,
    suffix: String = DefaultSuffix,
    compression: CompressionCodecName = DefaultCompression,
    conf: Configuration = DefaultConfiguration,
    filenamePolicySupplier: FilenamePolicySupplier = DefaultFilenamePolicySupplier,
    prefix: String = DefaultPrefix,
    shardNameTemplate: String = DefaultShardNameTemplate,
    tempDirectory: String = DefaultTempDirectory,
    datumFactory: AvroDatumFactory[T] = DefaultDatumFactory
  )
}

final case class ParquetGenericRecordIO(
  path: String,
  schema: Schema
) extends ParquetAvroIO[GenericRecord] {
  override protected def defaultDatumFactory: AvroDatumFactory[GenericRecord] =
    GenericRecordDatumFactory
  override protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[GenericRecord]] =
    ParquetAvroRead.readAvroGenericRecordFiles(schema, projection, predicate, configuration)
  override def tap(read: ReadP): Tap[GenericRecord] =
    ParquetGenericRecordTap(path, schema, read)

}

object ParquetGenericRecordIO {
  type ReadParam = ParquetAvroIO.ReadParam[GenericRecord]
  val ReadParam = ParquetAvroIO.ReadParam
  type WriteParam = ParquetAvroIO.WriteParam[GenericRecord]
  val WriteParam = ParquetAvroIO.WriteParam
}

final case class ParquetSpecificRecordIO[T <: SpecificRecord: ClassTag](path: String)
    extends ParquetAvroIO[T] {

  private lazy val recordClass: Class[T] = ScioUtil.classOf[T]
  override protected val schema: Schema =
    SpecificData.get().getSchema(recordClass)
  override protected val defaultDatumFactory: AvroDatumFactory[T] =
    new SpecificRecordDatumFactory(recordClass)

  override protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]] =
    ParquetAvroRead.readAvroFiles[T](projection, predicate, configuration)
  override def tap(read: ReadP): Tap[T] =
    ParquetSpecificRecordTap(path, read)

}

object ParquetSpecificRecordIO {
  type ReadParam[T] = ParquetAvroIO.ReadParam[T]
  val ReadParam = ParquetAvroIO.ReadParam
  type WriteParam[T] = ParquetAvroIO.WriteParam[T]
  val WriteParam = ParquetAvroIO.WriteParam
}
