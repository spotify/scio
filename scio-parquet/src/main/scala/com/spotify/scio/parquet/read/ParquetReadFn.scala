package com.spotify.scio.parquet.read

import com.spotify.scio.parquet.BeamInputFile
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

class ParquetReadFn[T, R](
  readSupportFactory: ReadSupportFactory[T],
  conf: SerializableConfiguration,
  projectionFn: SerializableFunction[T, R]
) extends DoFn[ReadableFile, R] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val SPLIT_LIMIT = 64000000L

  private def parquetFileReader(file: ReadableFile): ParquetFileReader = {
    val options = HadoopReadOptions.builder(conf.get()).build
    ParquetFileReader.open(BeamInputFile.of(file.openSeekable), options)
  }

  @GetRestrictionCoder def getRestrictionCoder = new OffsetRange.Coder

  @GetInitialRestriction
  def getInitialRestriction(@Element file: ReadableFile): OffsetRange = {
    val reader = parquetFileReader(file)

    try {
      new OffsetRange(0, reader.getRowGroups.size)
    } finally {
      reader.close()
    }
  }

  @NewTracker def newTracker(@Restriction restriction: OffsetRange) =
    new OffsetRangeTracker(restriction)

  @GetSize
  def getSize(@Element file: ReadableFile, @Restriction restriction: OffsetRange): Double = {
    val (_, size) = getRecordCountAndSize(file, restriction)
    size
  }

  @SplitRestriction
  def split(
    @Restriction restriction: OffsetRange,
    output: DoFn.OutputReceiver[OffsetRange],
    @Element file: ReadableFile
  ): Unit = {
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

  @ProcessElement
  def processElement(
    @Element file: ReadableFile,
    tracker: RestrictionTracker[OffsetRange, Long],
    outputReceiver: DoFn.OutputReceiver[R]
  ): Unit = {
    logger.debug(
      "reading file from offset {} to {}",
      tracker.currentRestriction.getFrom,
      tracker.currentRestriction.getTo
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

      var currentRowGroupIndex = tracker.currentRestriction.getFrom
      (0L until currentRowGroupIndex).foreach(_ => reader.skipNextRowGroup())

      while (tracker.tryClaim(currentRowGroupIndex)) {
        val pages = reader.readNextRowGroup

        logger.debug(
          "row group {} read in memory. row count = {}",
          currentRowGroupIndex,
          pages.getRowCount
        )

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
          outputReceiver,
          projectionFn
        )

        logger.debug(
          "Finish processing {} rows from row group {} in file {}",
          pages.getRowCount.toString,
          currentRowGroupIndex.toString,
          file.toString
        )

        currentRowGroupIndex += 1
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

          if (currentSize > SPLIT_LIMIT || endGroup - 1 == end) {
            (new OffsetRange(start, end + 1) +: offsets, end + 1, 0.0)
          } else {
            (offsets, start, currentSize)
          }
      }

    offsets.reverse
  }

}
