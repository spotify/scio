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

package com.spotify.scio.io

import com.google.common.io.CountingInputStream

import java.io.{BufferedInputStream, InputStream, OutputStream}
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.BinaryIO.BytesSink
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.ByteArrayCoder
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, ResourceId}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.util.MimeTypes
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.typelevel.scalaccompat.annotation.unused

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * A ScioIO class for writing raw bytes to files. Like TextIO, but without newline delimiters and
 * operating over Array[Byte] instead of String.
 * @param path
 *   a path to write to.
 */
final case class BinaryIO(path: String) extends ScioIO[Array[Byte]] {
  override type ReadP = BinaryIO.ReadParam
  override type WriteP = BinaryIO.WriteParam
  // write options are insufficient to derive a reader for the output files
  override val tapT: TapT.Aux[Array[Byte], Nothing] = EmptyTapOf[Array[Byte]]

  override def testId: String = s"BinaryIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Array[Byte]] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val coder = ByteArrayCoder.of()
    val srcFn = Functions.serializableFn { path: String =>
      new BinaryIO.BinarySource(path, params.emptyMatchTreatment, params.reader)
    }

    sc.withName("Create filepattern")
      .parallelize(List(filePattern))
      .applyTransform("Match All", FileIO.matchAll())
      .applyTransform(
        "Read Matches",
        FileIO
          .readMatches()
          .withCompression(params.compression)
          .withDirectoryTreatment(DirectoryTreatment.PROHIBIT)
      )
      .applyTransform(
        "Read all via FileBasedSource",
        // Setting desiredBundleSizeBytes to Long.MaxValue prevents Beam from trying to split files
        new ReadAllViaFileBasedSource[Array[Byte]](Long.MaxValue, srcFn, coder)
      )
  }

  private def binaryOut(
    path: String,
    suffix: String,
    numShards: Int,
    compression: Compression,
    header: Array[Byte],
    footer: Array[Byte],
    framePrefix: Array[Byte] => Array[Byte],
    frameSuffix: Array[Byte] => Array[Byte],
    filenamePolicySupplier: FilenamePolicySupplier,
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean,
    tempDirectory: ResourceId
  ): WriteFiles[Array[Byte], Void, Array[Byte]] = {
    require(tempDirectory != null, "tempDirectory must not be null")
    val fp = FilenamePolicySupplier.resolve(
      filenamePolicySupplier = filenamePolicySupplier,
      prefix = prefix,
      shardNameTemplate = shardNameTemplate,
      isWindowed = isWindowed
    )(ScioUtil.strippedPath(path), suffix)
    val dynamicDestinations = DynamicFileDestinations
      .constant(fp, SerializableFunctions.identity[Array[Byte]])
    val sink = new BytesSink(
      header,
      footer,
      framePrefix,
      frameSuffix,
      tempDirectory,
      dynamicDestinations,
      compression
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[Array[Byte]], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      binaryOut(
        path,
        params.suffix,
        params.numShards,
        params.compression,
        params.header,
        params.footer,
        params.framePrefix,
        params.frameSuffix,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context)
      )
    )
    EmptyTap
  }

  override def tap(read: BinaryIO.ReadParam): Tap[Nothing] = EmptyTap
}

object BinaryIO {

  private[scio] def openInputStreamsFor(
    pattern: String,
    tryDecompress: Boolean = true
  ): Iterator[InputStream] = {
    lazy val factory = new CompressorStreamFactory()

    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      Try(factory.createCompressorInputStream(buffered)).getOrElse(buffered)
    }

    val inputStreams = listFiles(pattern).map(getObjectInputStream)
    if (tryDecompress) {
      inputStreams.map(wrapInputStream).iterator
    } else {
      inputStreams.iterator
    }
  }

  private def listFiles(pattern: String): Seq[Metadata] =
    FileSystems.`match`(pattern).metadata().iterator.asScala.toSeq

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))

  object WriteParam {
    val DefaultPrefix: String = null
    val DefaultSuffix: String = ".bin"
    val DefaultNumShards: Int = 0
    val DefaultCompression: Compression = Compression.UNCOMPRESSED
    val DefaultHeader: Array[Byte] = Array.emptyByteArray
    val DefaultFooter: Array[Byte] = Array.emptyByteArray
    val DefaultShardNameTemplate: String = null
    val DefaultFramePrefix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
    val DefaultFrameSuffix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
    val DefaultTempDirectory: String = null
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
  }

  final case class WriteParam private (
    prefix: String = WriteParam.DefaultPrefix,
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Array[Byte] = WriteParam.DefaultHeader,
    footer: Array[Byte] = WriteParam.DefaultFooter,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    framePrefix: Array[Byte] => Array[Byte] = WriteParam.DefaultFramePrefix,
    frameSuffix: Array[Byte] => Array[Byte] = WriteParam.DefaultFrameSuffix,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier
  )

  final private class BytesSink(
    val header: Array[Byte],
    val footer: Array[Byte],
    val framePrefix: Array[Byte] => Array[Byte],
    val frameSuffix: Array[Byte] => Array[Byte],
    val tempDirectory: ResourceId,
    val dynamicDestinations: FileBasedSink.DynamicDestinations[Array[Byte], Void, Array[Byte]],
    val compression: Compression
  ) extends FileBasedSink[Array[Byte], Void, Array[Byte]](
        StaticValueProvider.of(tempDirectory),
        dynamicDestinations,
        compression
      ) {
    override def createWriteOperation(): FileBasedSink.WriteOperation[Void, Array[Byte]] = {
      new FileBasedSink.WriteOperation[Void, Array[Byte]](this) {
        override def createWriter(): FileBasedSink.Writer[Void, Array[Byte]] = {
          new FileBasedSink.Writer[Void, Array[Byte]](this, MimeTypes.BINARY) {
            @transient private var channel: OutputStream = _
            override def prepareWrite(channel: WritableByteChannel): Unit = this.channel =
              Channels.newOutputStream(channel)
            override def writeHeader(): Unit = this.channel.write(header)
            override def writeFooter(): Unit = this.channel.write(footer)

            override def write(value: Array[Byte]): Unit = {
              if (this.channel == null) {
                throw new IllegalStateException(
                  "Trying to write to a BytesSink that has not been opened"
                )
              }
              this.channel.write(framePrefix(value))
              this.channel.write(value)
              this.channel.write(frameSuffix(value))
            }

            override def finishWrite(): Unit = {
              if (this.channel == null) {
                throw new IllegalStateException(
                  "Trying to flush a BytesSink that has not been opened"
                )
              }
              this.channel.flush()
            }
          }
        }
      }
    }
  }

  object ReadParam {
    val DefaultSuffix: String = ".bin"
    val DefaultCompression: Compression = Compression.UNCOMPRESSED
    val DefaultEmptyMatchTreatment: EmptyMatchTreatment = EmptyMatchTreatment.DISALLOW
  }

  final case class ReadParam private (
    reader: BinaryFileReader,
    compression: Compression = ReadParam.DefaultCompression,
    emptyMatchTreatment: EmptyMatchTreatment = ReadParam.DefaultEmptyMatchTreatment,
    suffix: String = ReadParam.DefaultSuffix
  )

  /** Trait for reading binary file formats */
  trait BinaryFileReader {

    /**
     * State constructed during `start` and maintained through reading, potentially updated during
     * each record read
     */
    type State

    /** Read any header and construct the initial read state. */
    def start(is: InputStream): State

    /**
     * @return
     *   The updated read state, record bytes. Return null for record byte array when no record is
     *   read.
     */
    def readRecord(state: State, is: InputStream): (State, Array[Byte])

    /** Read any footer and perform any required validation after the last record is read. */
    def end(@unused state: State, @unused is: InputStream): Unit = ()
  }

  final private[scio] class BinarySingleFileSource(
    binaryFileReader: BinaryFileReader,
    metadata: Metadata,
    start: Long,
    end: Long
  ) extends FileBasedSource[Array[Byte]](metadata, Long.MaxValue, start, end) {
    override def isSplittable: Boolean = false

    override def createForSubrangeOfFile(
      fileMetadata: Metadata,
      start: Long,
      end: Long
    ): FileBasedSource[Array[Byte]] =
      throw new NotImplementedError()

    override def createSingleFileReader(
      options: PipelineOptions
    ): FileBasedSource.FileBasedReader[Array[Byte]] =
      new FileBasedSource.FileBasedReader[Array[Byte]](this) {
        var is: CountingInputStream = _
        var state: binaryFileReader.State = _
        var startOfRecord: Long = _
        var current: Option[Array[Byte]] = None

        // exception matches method contract
        override def getCurrentOffset: Long = current.map(_ => startOfRecord).get

        // exception matches method contract
        override def getCurrent: Array[Byte] = current.get

        override def startReading(channel: ReadableByteChannel): Unit = {
          is = new CountingInputStream(Channels.newInputStream(channel))
          val newState = binaryFileReader.start(is)
          state = newState
        }

        override def readNextRecord(): Boolean = {
          startOfRecord = is.getCount + 1
          val (newState, record) = binaryFileReader.readRecord(state, is)
          state = newState
          current = Option(record)
          current match {
            case Some(_) => true
            case None    =>
              binaryFileReader.end(state, is)
              false
          }
        }

        override def allowsDynamicSplitting(): Boolean =
          false
      }
  }

  final private[scio] class BinarySource(
    path: String,
    emptyMatchTreatment: EmptyMatchTreatment,
    binaryFileReader: BinaryFileReader
  ) extends FileBasedSource[Array[Byte]](
        StaticValueProvider.of(path),
        emptyMatchTreatment,
        Long.MaxValue
      ) {
    override def isSplittable: Boolean = false

    override def createForSubrangeOfFile(
      fileMetadata: Metadata,
      start: Long,
      end: Long
    ): FileBasedSource[Array[Byte]] = {
      require(
        start == 0,
        s"Range with offset $start requested, but BinaryIO is unsplittable"
      )
      new BinarySingleFileSource(binaryFileReader, fileMetadata, start, end)
    }

    override def createSingleFileReader(
      options: PipelineOptions
    ): FileBasedSource.FileBasedReader[Array[Byte]] = throw new NotImplementedError()
  }
}
