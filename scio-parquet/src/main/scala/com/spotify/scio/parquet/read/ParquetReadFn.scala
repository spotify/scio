/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.parquet.read

import com.spotify.scio.parquet.BeamInputFile
import com.spotify.scio.parquet.read.ParquetReadFn._
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.splittabledofn.{OffsetRangeTracker, RestrictionTracker}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.io.{ColumnIOFactory, ParquetDecodingException, RecordReader}
import org.apache.parquet.HadoopReadOptions
import org.slf4j.LoggerFactory

import java.util.{Set => JSet}
import scala.jdk.CollectionConverters._

object ParquetReadFn {
  sealed private trait Granularity
  private case object File extends Granularity
  private case object RowGroup extends Granularity

  // Constants
  private val SplitLimit = 64000000L
  private val EntireFileRange = new OffsetRange(0, 1)
}

class ParquetReadFn[T, R](
  readSupportFactory: ReadSupportFactory[T],
  conf: SerializableConfiguration,
  projectionFn: SerializableFunction[T, R]
) extends DoFn[ReadableFile, R] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val granularity =
    conf.get().get(ParquetReadConfiguration.SplitGranularity) match {
      case ParquetReadConfiguration.SplitGranularityFile     => File
      case ParquetReadConfiguration.SplitGranularityRowGroup => RowGroup
      case _                                                 => File
    }

  private def parquetFileReader(file: ReadableFile): ParquetFileReader = {
    val options = HadoopReadOptions.builder(conf.get()).build
    ParquetFileReader.open(BeamInputFile.of(file.openSeekable), options)
  }

  @GetRestrictionCoder def getRestrictionCoder = new OffsetRange.Coder

  @GetInitialRestriction
  def getInitialRestriction(@Element file: ReadableFile): OffsetRange = granularity match {
    case File => EntireFileRange
    case RowGroup =>
      val reader = parquetFileReader(file)
      val rowGroups =
        try {
          reader.getRowGroups.size()
        } finally {
          reader.close()
        }
      new OffsetRange(0, rowGroups)
  }

  @NewTracker def newTracker(@Restriction restriction: OffsetRange) =
    new OffsetRangeTracker(restriction)

  @GetSize
  def getSize(@Element file: ReadableFile, @Restriction restriction: OffsetRange): Double = {
    granularity match {
      case File => file.getMetadata.sizeBytes().toDouble
      case RowGroup =>
        val (_, size) = getRecordCountAndSize(file, restriction)
        size
    }
  }

  @SplitRestriction
  def split(
    @Restriction restriction: OffsetRange,
    output: DoFn.OutputReceiver[OffsetRange],
    @Element file: ReadableFile
  ): Unit = {
    granularity match {
      case File => output.output(EntireFileRange)
      case RowGroup =>
        val reader = parquetFileReader(file)
        try {
          splitRowGroupsWithLimit(
            restriction.getFrom,
            restriction.getTo,
            reader.getRowGroups.asScala.toSeq
          ).foreach(output.output)
        } finally {
          reader.close()
        }
    }
  }

  @ProcessElement
  def processElement(
    @Element file: ReadableFile,
    tracker: RestrictionTracker[OffsetRange, Long],
    out: DoFn.OutputReceiver[R]
  ): Unit = {
    logger.debug(
      "reading file from offset {} to {}",
      tracker.currentRestriction.getFrom,
      if (granularity == File) "end" else tracker.currentRestriction().getTo
    )
    val options = HadoopReadOptions.builder(conf.get()).build

    val reader = ParquetFileReader.open(BeamInputFile.of(file.openSeekable), options)
    try {
      val filter = options.getRecordFilter
      val hadoopConf = options.asInstanceOf[HadoopReadOptions].getConf
      val parquetFileMetadata = reader.getFooter.getFileMetaData
      val fileSchema = parquetFileMetadata.getSchema
      val fileMetadata = parquetFileMetadata.getKeyValueMetaData
      val readSupport = readSupportFactory.readSupport

      val readContext = readSupport.init(
        new InitContext(
          hadoopConf,
          fileMetadata.asScala.map { case (k, v) =>
            k -> (ImmutableSet.of(v): JSet[String])
          }.asJava,
          fileSchema
        )
      )
      reader.setRequestedSchema(readContext.getRequestedSchema)

      val columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy)
      val recordConverter =
        readSupport.prepareForRead(hadoopConf, fileMetadata, fileSchema, readContext)
      val columnIO = columnIOFactory.getColumnIO(readContext.getRequestedSchema, fileSchema, true)

      granularity match {
        case File =>
          val tryClaim = tracker.tryClaim(tracker.currentRestriction().getFrom)
          var pages = reader.readNextFilteredRowGroup()
          // Must check tryClaim before reading so work isn't duplicated across workers
          while (tryClaim && pages != null) {
            val recordReader = columnIO.getRecordReader(
              pages,
              recordConverter,
              if (options.useRecordFilter) filter else FilterCompat.NOOP
            )
            readRowGroup(
              0,
              pages.getRowCount,
              file,
              recordReader,
              out,
              projectionFn
            )
            pages = reader.readNextFilteredRowGroup()
          }
        case RowGroup =>
          var currentRowGroupIndex = tracker.currentRestriction.getFrom
          (0L until currentRowGroupIndex).foreach(_ => reader.skipNextRowGroup())
          while (tracker.tryClaim(currentRowGroupIndex)) {
            val pages = reader.readNextFilteredRowGroup()

            val recordReader = columnIO.getRecordReader(
              pages,
              recordConverter,
              if (options.useRecordFilter) filter else FilterCompat.NOOP
            )
            readRowGroup(
              currentRowGroupIndex,
              pages.getRowCount,
              file,
              recordReader,
              out,
              projectionFn
            )

            currentRowGroupIndex += 1
          }
      }
    } finally {
      reader.close()
    }
  }

  private def readRowGroup(
    rowGroupIndex: Long,
    rowCount: Long,
    file: ReadableFile,
    recordReader: RecordReader[T],
    outputReceiver: DoFn.OutputReceiver[R],
    projectionFn: SerializableFunction[T, R]
  ): Unit = {
    logger.debug(
      "row group {} read in memory. row count = {}",
      rowGroupIndex,
      rowCount
    )
    var currentRow = 0L
    while (currentRow < rowCount) {
      try {
        val record = recordReader.read
        if (record == null) {
          // it happens when a record is filtered out in this block
          logger.debug(
            "record is filtered out by reader in row group {} in file {}",
            rowGroupIndex,
            file.toString
          )
        } else if (recordReader.shouldSkipCurrentRecord) {
          // this record is being filtered via the filter2 package
          logger.debug(
            "skipping record at {} in row group {} in file {}",
            currentRow.toString,
            rowGroupIndex.toString,
            file.toString
          )

        } else {
          outputReceiver.output(projectionFn(record))
        }

        currentRow += 1
      } catch {
        case e: RuntimeException =>
          throw new ParquetDecodingException(
            s"Can not read value at $currentRow in row group" +
              s" $rowGroupIndex in file $file",
            e
          )
      }
    }
    logger.debug(
      "Finish processing {} rows from row group {} in file {}",
      rowCount.toString,
      rowGroupIndex.toString,
      file.toString
    )
  }

  private def getRecordCountAndSize(file: ReadableFile, restriction: OffsetRange) = {
    val reader = parquetFileReader(file)

    try {
      reader.getRowGroups.asScala
        .slice(restriction.getFrom.toInt, restriction.getTo.toInt)
        .foldLeft((0.0, 0.0)) { case ((count, size), rowGroup) =>
          (count + rowGroup.getRowCount, size + rowGroup.getTotalByteSize)
        }
    } finally {
      reader.close()
    }
  }

  def splitRowGroupsWithLimit(
    startGroup: Long,
    endGroup: Long,
    rowGroups: Seq[BlockMetaData]
  ): Seq[OffsetRange] = {
    val (offsets, _, _) = rowGroups.zipWithIndex
      .slice(startGroup.toInt, endGroup.toInt)
      .foldLeft((Seq.empty[OffsetRange], startGroup.toInt, 0.0)) {
        case ((offsets, start, size), (rowGroup, end)) =>
          val currentSize = size + rowGroup.getTotalByteSize

          if (currentSize > SplitLimit || endGroup - 1 == end) {
            (new OffsetRange(start, end + 1) +: offsets, end + 1, 0.0)
          } else {
            (offsets, start, currentSize)
          }
      }

    offsets.reverse
  }
}
