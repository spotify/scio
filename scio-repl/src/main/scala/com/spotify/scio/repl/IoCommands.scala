/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.repl

import java.io._
import java.net.URI
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.util.GcsUtil
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath
import com.spotify.scio.util.ScioUtil
import kantan.csv.{RowDecoder, RowEncoder}
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase}
import org.apache.commons.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Commands for simple file I/O in the REPL. */
class IoCommands(options: PipelineOptions) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[IoCommands])

  private val TEXT = "text/plain"
  private val BINARY = "application/octet-stream"

  // TODO: figure out how to support HDFS without messing up dependencies
  private lazy val gcsUtil: GcsUtil = new GcsUtilFactory().create(options)

  // =======================================================================
  // Read operations
  // =======================================================================

  /** Read from an Avro file on local filesystem or GCS. */
  def readAvro[T : ClassTag](path: String): Iterator[T] = {
    val cls = ScioUtil.classOf[T]
    val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      new SpecificDatumReader[T]()
    } else {
      new GenericDatumReader[T]()
    }
    new DataFileStream[T](inputStream(path), reader).iterator().asScala
  }

  /** Read from a text file on local filesystem or GCS. */
  def readText(path: String): Iterator[String] =
    IOUtils.lineIterator(inputStream(path), StandardCharsets.UTF_8).asScala

  /** Read from a CSV file on local filesystem or GCS. */
  def readCsv[T: RowDecoder](path: String,
                             sep: Char = ',',
                             header: Boolean = false): Iterator[T] = {
    import kantan.csv.ops._
    implicit val codec = scala.io.Codec.UTF8
    inputStream(path).asUnsafeCsvReader[T](sep, header).toIterator
  }

  /** Read from a TSV file on local filesystem or GCS. */
  def readTsv[T: RowDecoder](path: String,
                             sep: Char = '\t',
                             header: Boolean = false): Iterator[T] = {
    import kantan.csv.ops._
    implicit val codec = scala.io.Codec.UTF8
    inputStream(path).asUnsafeCsvReader[T](sep, header).toIterator
  }

  // =======================================================================
  // Write operations
  // =======================================================================

  private def plural[T](data: Seq[T]): String = if (data.size > 1) "s" else ""

  /** Write to an Avro file on local filesystem or GCS. */
  def writeAvro[T: ClassTag](path: String, data: Seq[T]): Unit = {
    val cls = ScioUtil.classOf[T]
    val (writer, schema) = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      (new SpecificDatumWriter[T](cls), data.head.asInstanceOf[SpecificRecordBase].getSchema)
    } else {
      (new GenericDatumWriter[T](), data.head.asInstanceOf[GenericRecord].getSchema)
    }
    val fileWriter = new DataFileWriter[T](writer).create(schema, outputStream(path, BINARY))
    data.foreach(fileWriter.append)
    fileWriter.close()
    logger.info("{} record{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a text file on local filesystem or GCS. */
  def writeText(path: String, data: Seq[String]): Unit = {
    IOUtils.writeLines(
      data.asJava, IOUtils.LINE_SEPARATOR, outputStream(path, TEXT), StandardCharsets.UTF_8)
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a CSV file on local filesystem or GCS. */
  def writeCsv[T: RowEncoder](path: String, data: Seq[T],
                              sep: Char = ',',
                              header: Seq[String] = Seq.empty): Unit = {
    import kantan.csv.ops._
    IOUtils.write(data.asCsv(sep, header: _*), outputStream(path, TEXT), StandardCharsets.UTF_8)
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a TSV file on local filesystem or GCS. */
  def writeTsv[T: RowEncoder](path: String, data: Seq[T],
                              sep: Char = '\t',
                              header: Seq[String] = Seq.empty): Unit = {
    import kantan.csv.ops._
    IOUtils.write(data.asCsv(sep, header: _*), outputStream(path, TEXT), StandardCharsets.UTF_8)
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  // =======================================================================
  // Utilities
  // =======================================================================

  private def inputStream(path: String): InputStream = {
    val uri = new URI(path)
    if (ScioUtil.isGcsUri(uri)) {
      Channels.newInputStream(gcsUtil.open(GcsPath.fromUri(uri)))
    } else if (ScioUtil.isLocalUri(uri)) {
      new FileInputStream(path)
    } else {
      throw new IllegalArgumentException(s"Unsupported path $path")
    }
  }

  private def outputStream(path: String, contentType: String): OutputStream = {
    val uri = new URI(path)
    if (ScioUtil.isGcsUri(uri)) {
      Channels.newOutputStream(gcsUtil.create(GcsPath.fromUri(uri), contentType))
    } else if (ScioUtil.isLocalUri(uri)) {
      new FileOutputStream(path)
    } else {
      throw new IllegalArgumentException(s"Unsupported path $path")
    }
  }

}
