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
import com.spotify.scio.avro.{GenericRecordDatumFactory, SpecificRecordDatumFactory, avroCoder}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT, TestIO}
import com.spotify.scio.parquet.avro.ParquetAvroIO.ReadParam.{DefaultConfiguration, DefaultPredicate, DefaultProjection, DefaultSuffix}
import com.spotify.scio.parquet.avro.ParquetAvroIO.WriteParam.{DefaultCompression, DefaultFilenamePolicySupplier, DefaultNumShards, DefaultPrefix, DefaultShardNameTemplate, DefaultTempDirectory}
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil, ParquetConfiguration}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord}
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
import scala.reflect.{classTag, ClassTag}

sealed trait ParquetAvroIO[T <: IndexedRecord] extends ScioIO[T] {

  override type ReadP = ParquetAvroIO.ReadParam
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  def path: String
  protected def schema: Schema

  protected def datumFactory: AvroDatumFactory[T]

  protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]]

  implicit lazy val coder: Coder[T] = avroCoder(datumFactory, schema)

  override def testId: String = s"ParquetAvroIO($path)"

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
    val bCoder = CoderMaterializer.beam(sc, coder)
    val transform = new PTransform[PBegin, PCollection[T]] {
      override def expand(input: PBegin): PCollection[T] = {
        input
          .apply(FileIO.`match`().filepattern(path))
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
    val recordClass = datumFactory.getType
    conf.setClass("key.class", classOf[Void], classOf[Void])
    conf.setClass("value.class", recordClass, recordClass)
    AvroReadSupport.setAvroReadSchema(conf, schema)

    if (recordClass == classOf[GenericRecord]) {
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    }

    Option(params.projection).foreach(p => AvroReadSupport.setRequestedProjection(conf, p))
    Option(params.predicate).foreach(p => ParquetInputFormat.setFilterPredicate(conf, p))

    val job = Job.getInstance(conf)
    GcsConnectorUtil.setInputPaths(sc, job, path)
    job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
    val transform = HadoopFormatIO
      .read[Void, T]()
      // Force coders for hadoop job
      .withKeyTranslation(ParquetAvroIO.Identity[Void], CoderMaterializer.beam(sc, Coder.voidCoder))
      .withValueTranslation(
        ParquetAvroIO.Identity(datumFactory.getType),
        CoderMaterializer.beam(sc, coder)
      )
      .withConfiguration(job.getConfiguration)

    sc.applyTransform(transform).map(_.getValue)
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[T] = {
    // The projection function is not part of the test input, so it must be applied directly
    val projectedFields = Option(params.projection).map(_.getFields.asScala.map(_.name()).toSet)
    TestDataManager
      .getInput(sc.testId.get)(this)
      .toSCollection(sc)
      .map { record =>
        projectedFields match {
          case None => record
          case Some(projection) =>
            record
              .getSchema
              .getFields
              .asScala
              .foldLeft(GenericData.get().deepCopy(record.getSchema, record)) { (r, f) =>
                val names = Set(f.name()) ++ f.aliases().asScala.toSet
                if (projection.intersect(names).isEmpty) {
                  // field is not part of the projection. set default value
                  val i = f.pos()
                  val v = GenericData.get().getDefaultValue(f)
                  r.put(i, v)
                }
                r
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
    val job = Job.getInstance(ParquetConfiguration.ofNullable(conf))
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
      extends SimpleFunction[T, T](SerializableFunctions.identity) {
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

    private[scio] def apply(params: WriteParam): ReadParam =
      new ReadParam(
        projection = DefaultProjection,
        predicate = DefaultPredicate,
        conf = params.conf,
        suffix = params.suffix
      )
  }

  final case class ReadParam private (
    projection: Schema = DefaultProjection,
    predicate: FilterPredicate = DefaultPredicate,
    conf: Configuration = DefaultConfiguration,
    suffix: String = DefaultSuffix
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
  }

  final case class WriteParam private (
    numShards: Int = DefaultNumShards,
    suffix: String = DefaultSuffix,
    compression: CompressionCodecName = DefaultCompression,
    conf: Configuration = DefaultConfiguration,
    filenamePolicySupplier: FilenamePolicySupplier = DefaultFilenamePolicySupplier,
    prefix: String = DefaultPrefix,
    shardNameTemplate: String = DefaultShardNameTemplate,
    tempDirectory: String = DefaultTempDirectory
  )

//  private[avro] def containsLogicalType(s: Schema): Boolean = {
//    s.getLogicalType != null || (s.getType match {
//      case Schema.Type.RECORD => s.getFields.asScala.exists(f => containsLogicalType(f.schema()))
//      case Schema.Type.ARRAY  => containsLogicalType(s.getElementType)
//      case Schema.Type.UNION  => s.getTypes.asScala.exists(t => containsLogicalType(t))
//      case Schema.Type.MAP    => containsLogicalType(s.getValueType)
//      case _                  => false
//    })
//  }
}

final case class ParquetGenericRecordIO(
  path: String,
  schema: Schema,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory
) extends ParquetAvroIO[GenericRecord] {
  override protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[GenericRecord]] =
    ParquetAvroRead.readAvroGenericRecordFiles(schema, projection, predicate, configuration)
  override def tap(read: ReadP): Tap[GenericRecord] =
    ParquetGenericRecordTap(path, schema, datumFactory, read)
}

object ParquetGenericRecordIO {
  type ReadParam = ParquetAvroIO.ReadParam
  val ReadParam = ParquetAvroIO.ReadParam
  type WriteParam = ParquetAvroIO.WriteParam
  val WriteParam = ParquetAvroIO.WriteParam
}

final case class ParquetSpecificRecordIO[T <: SpecificRecord: ClassTag](
  path: String,
  datumFactory: AvroDatumFactory[T]
) extends ParquetAvroIO[T] {

  override protected val schema: Schema = SpecificData.get().getSchema(ScioUtil.classOf[T])

  override protected def readFiles(
    projection: Schema,
    predicate: FilterPredicate,
    configuration: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]] =
    ParquetAvroRead.readAvroFiles[T](projection, predicate, configuration)
  override def tap(read: ReadP): Tap[T] =
    ParquetSpecificRecordTap(path, datumFactory, read)
}

object ParquetSpecificRecordIO {
  private[scio] def defaultDatumFactory[T <: SpecificRecord: ClassTag]: AvroDatumFactory[T] =
    new SpecificRecordDatumFactory[T](ScioUtil.classOf[T])

  type ReadParam = ParquetAvroIO.ReadParam
  val ReadParam = ParquetAvroIO.ReadParam
  type WriteParam = ParquetAvroIO.WriteParam
  val WriteParam = ParquetAvroIO.WriteParam

  def apply[T <: SpecificRecord: ClassTag](path: String): ParquetSpecificRecordIO[T] =
    ParquetSpecificRecordIO(path, defaultDatumFactory)
}

sealed trait ParquetAvroTap[T <: IndexedRecord] extends Tap[T] {
  def path: String
  def schema: Schema
  def datumFactory: AvroDatumFactory[T]
  def params: ParquetAvroIO.ReadParam

  override def value: Iterator[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    val modelData = datumFactory(schema, schema) match {
      case reader: GenericDatumReader[_] => Some(reader.getData)
      case _                             => None
    }
    xs.iterator.flatMap { metadata =>
      val reader = AvroParquetReader
        .builder[T](BeamInputFile.of(metadata.resourceId()))
        .withDataModel(modelData.orNull)
        .withConf(ParquetConfiguration.ofNullable(params.conf))
        .build()

      new Iterator[T] {
        private var current: T = reader.read()

        override def hasNext: Boolean = current != null

        override def next(): T = {
          val prev = current
          current = reader.read()
          prev
        }
      }
    }
  }
}

final case class ParquetGenericRecordTap(
  path: String,
  schema: Schema,
  datumFactory: AvroDatumFactory[GenericRecord] = GenericRecordDatumFactory,
  params: ParquetGenericRecordIO.ReadParam = ParquetGenericRecordIO.ReadParam()
) extends ParquetAvroTap[GenericRecord] {
  override def open(sc: ScioContext): SCollection[GenericRecord] =
    sc.read(ParquetGenericRecordIO(path, schema, datumFactory))(params)
}

final case class ParquetSpecificRecordTap[T <: SpecificRecord: ClassTag](
  path: String,
  datumFactory: AvroDatumFactory[T],
  params: ParquetSpecificRecordIO.ReadParam
) extends ParquetAvroTap[T] {
  override def schema: Schema = SpecificData.get().getSchema(ScioUtil.classOf[T])
  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ParquetSpecificRecordIO[T](path, datumFactory))(params)
}

object ParquetSpecificRecordTap {
  def apply[T <: SpecificRecord: ClassTag](path: String): ParquetSpecificRecordTap[T] =
    new ParquetSpecificRecordTap(
      path,
      ParquetSpecificRecordIO.defaultDatumFactory,
      ParquetSpecificRecordIO.ReadParam()
    )
}
