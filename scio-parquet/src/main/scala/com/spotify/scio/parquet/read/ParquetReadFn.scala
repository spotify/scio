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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.io.{ColumnIOFactory, ParquetDecodingException, RecordReader}
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.page.PageReadStore
import org.slf4j.LoggerFactory

import java.util.{Set => JSet}
import scala.jdk.CollectionConverters._

object ParquetReadFn {
  @transient
  private lazy val logger = LoggerFactory.getLogger(classOf[ParquetReadFn[_, _]])

  sealed private trait SplitGranularity
  private object SplitGranularity {
    case object File extends SplitGranularity
    case object RowGroup extends SplitGranularity
  }

  sealed abstract private class FilterGranularity(
    val readNextRowGroup: ParquetFileReader => PageReadStore
  )
  private object FilterGranularity {
    case object Page extends FilterGranularity(_.readNextFilteredRowGroup())
    case object Record extends FilterGranularity(_.readNextRowGroup())
  }

  // Constants
  private val SplitLimit = 64000000L
  private val EntireFileRange = new OffsetRange(0, 1)
}

class ParquetReadFn[T, R](
  readSupportFactory: ReadSupportFactory[T],
  conf: SerializableConfiguration,
  projectionFn: SerializableFunction[T, R]
) extends DoFn[ReadableFile, R] {
  @transient
  private lazy val options = HadoopReadOptions.builder(conf.get()).build()

  @transient
  private lazy val splitGranularity =
    conf
      .get()
      .get(
        ParquetReadConfiguration.SplitGranularity,
        ParquetReadConfiguration.SplitGranularityFile
      ) match {
      case ParquetReadConfiguration.SplitGranularityFile     => SplitGranularity.File
      case ParquetReadConfiguration.SplitGranularityRowGroup => SplitGranularity.RowGroup
      case other: String                                     =>
        logger.warn(
          "Found unsupported setting value {} for key {}. Defaulting to file-level splitting.",
          Array(other, ParquetReadConfiguration.SplitGranularity) // varargs fails with 2.12
        )
        SplitGranularity.File
    }

  @transient
  private lazy val filterGranularity =
    conf
      .get()
      .get(
        ParquetReadConfiguration.FilterGranularity,
        ParquetReadConfiguration.FilterGranularityRecord
      ) match {
      case ParquetReadConfiguration.FilterGranularityPage   => FilterGranularity.Page
      case ParquetReadConfiguration.FilterGranularityRecord => FilterGranularity.Record
      case other: String                                    =>
        logger.warn(
          "Found unsupported setting value {} for key {}. Defaulting to record-level filtering.",
          Array(other, ParquetReadConfiguration.FilterGranularity) // varargs fails with 2.12
        )
        FilterGranularity.Record
    }

  private def parquetFileReader(file: ReadableFile): ParquetFileReader =
    ParquetFileReader.open(BeamInputFile.of(file.openSeekable), options)

  @GetRestrictionCoder def getRestrictionCoder = new OffsetRange.Coder

  @GetInitialRestriction
  def getInitialRestriction(@Element file: ReadableFile): OffsetRange = splitGranularity match {
    case SplitGranularity.File     => EntireFileRange
    case SplitGranularity.RowGroup =>
      val reader = parquetFileReader(file)
      val rowGroups =
        try {
          reader.getRowGroups.size()
        } finally {
          reader.close()
        }
      new OffsetRange(0L, rowGroups.toLong)
  }

  @NewTracker def newTracker(@Restriction restriction: OffsetRange) =
    new OffsetRangeTracker(restriction)

  @GetSize
  def getSize(@Element file: ReadableFile, @Restriction restriction: OffsetRange): Double = {
    splitGranularity match {
      case SplitGranularity.File     => file.getMetadata.sizeBytes().toDouble
      case SplitGranularity.RowGroup => getRowGroupsSizeBytes(file, restriction).toDouble
    }
  }

  @SplitRestriction
  def split(
    @Restriction restriction: OffsetRange,
    output: DoFn.OutputReceiver[OffsetRange],
    @Element file: ReadableFile
  ): Unit = {
    splitGranularity match {
      case SplitGranularity.File     => output.output(EntireFileRange)
      case SplitGranularity.RowGroup =>
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
      if (splitGranularity == SplitGranularity.File) "end" else tracker.currentRestriction().getTo
    )
    val reader = parquetFileReader(file)
    try {
      val filter = options.getRecordFilter
      val parquetFileMetadata = reader.getFooter.getFileMetaData
      val fileSchema = parquetFileMetadata.getSchema
      val fileMetadata = parquetFileMetadata.getKeyValueMetaData
      val readSupport = readSupportFactory.readSupport

      val readContext = readSupport.init(
        new InitContext(
          conf.get(),
          fileMetadata.asScala.map { case (k, v) =>
            k -> (ImmutableSet.of(v): JSet[String])
          }.asJava,
          fileSchema
        )
      )
      reader.setRequestedSchema(readContext.getRequestedSchema)

      val columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy)
      val recordConverter =
        readSupport.prepareForRead(conf.get(), fileMetadata, fileSchema, readContext)
      val columnIO = columnIOFactory.getColumnIO(readContext.getRequestedSchema, fileSchema, true)

      splitGranularity match {
        case SplitGranularity.File =>
          val tryClaim = tracker.tryClaim(tracker.currentRestriction().getFrom)
          var pages = filterGranularity.readNextRowGroup(reader)
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
            pages = filterGranularity.readNextRowGroup(reader)
          }
        case SplitGranularity.RowGroup =>
          var currentRowGroupIndex = tracker.currentRestriction.getFrom
          (0L until currentRowGroupIndex).foreach(_ => reader.skipNextRowGroup())
          while (tracker.tryClaim(currentRowGroupIndex)) {
            val pages = filterGranularity.readNextRowGroup(reader)

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
        val record = recordReader.read()
        if (record == null) {
          // it happens when a record is filtered out in this block
          logger.debug(
            "record is filtered out by reader in row group {} in file {}",
            rowGroupIndex,
            file
          )
        } else if (recordReader.shouldSkipCurrentRecord) {
          // this record is being filtered via the filter2 package
          logger.debug(
            "skipping record at {} in row group {} in file {}",
            Array(currentRow, rowGroupIndex, file) // varargs fails with 2.12
          )

        } else {
          outputReceiver.output(projectionFn(record))
        }

        currentRow += 1
      } catch {
        case e: RuntimeException =>
          throw new ParquetDecodingException(
            s"Can not read value at $currentRow in row group $rowGroupIndex in file $file",
            e
          )
      }
    }
    logger.debug(
      "Finish processing {} rows from row group {} in file {}",
      Array(rowCount, rowGroupIndex, file) // varargs fails with 2.12
    )
  }

  private def getRowGroupsSizeBytes(file: ReadableFile, restriction: OffsetRange): Long = {
    val reader = parquetFileReader(file)
    try {
      reader.getRowGroups.asScala
        .slice(restriction.getFrom.toInt, restriction.getTo.toInt)
        .map(_.getTotalByteSize)
        .sum
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
      .foldLeft((Seq.newBuilder[OffsetRange], startGroup, 0L)) {
        case ((offsets, start, size), (rowGroup, end)) =>
          val currentSize = size + rowGroup.getTotalByteSize
          val next = end.toLong + 1

          if (currentSize > SplitLimit || endGroup - 1 == end) {
            (offsets += new OffsetRange(start, next), next, 0)
          } else {
            (offsets, start, currentSize)
          }
      }

    offsets.result()
  }
}
