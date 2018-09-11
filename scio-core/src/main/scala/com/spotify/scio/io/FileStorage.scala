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

package com.spotify.scio.io

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}
import java.util.Collections

import com.google.api.client.util.Charsets
import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableInput}
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[scio] object FileStorage {
  @inline final def apply(path: String): FileStorage = new FileStorage(path)
}

private[scio] final class FileStorage(protected[scio] val path: String) {

  private def listFiles: Seq[Metadata] = FileSystems.`match`(path).metadata().asScala

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))

  private def getAvroSeekableInput(meta: Metadata): SeekableInput =
    new SeekableInput {
      require(meta.isReadSeekEfficient)
      private val in = FileSystems.open(meta.resourceId()).asInstanceOf[SeekableByteChannel]
      override def read(b: Array[Byte], off: Int, len: Int): Int =
        in.read(ByteBuffer.wrap(b, off, len))
      override def tell(): Long = in.position()
      override def length(): Long = in.size()
      override def seek(p: Long): Unit = in.position(p)
      override def close(): Unit = in.close()
    }

  def avroFile[T: ClassTag](schema: Schema = null): Iterator[T] = {
    val cls = ScioUtil.classOf[T]
    val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      new SpecificDatumReader[T](cls)
    } else {
      new GenericDatumReader[T](schema)
    }

    listFiles.map(m => DataFileReader.openReader(getAvroSeekableInput(m), reader))
      .map(_.iterator().asScala).reduce(_ ++ _)
  }

  def textFile: Iterator[String] = {
    val factory = new CompressorStreamFactory()
    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      try {
        factory.createCompressorInputStream(buffered)
      } catch {
        case _: Throwable => buffered
      }
    }
    val input = getDirectoryInputStream(path, wrapInputStream)
    IOUtils.lineIterator(input, Charsets.UTF_8).asScala
  }

  def tableRowJsonFile: Iterator[TableRow] =
    textFile.map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))

  def isDone: Boolean = {
    val partPattern = "([0-9]{5})-of-([0-9]{5})".r
    val metadata = try {
      listFiles
    } catch {
      case _: FileNotFoundException => Seq.empty
    }
    val nums = metadata.flatMap { meta =>
      val m = partPattern.findAllIn(meta.resourceId().toString)
      if (m.hasNext) {
        Some((m.group(1).toInt, m.group(2).toInt))
      } else {
        None
      }
    }

    if (metadata.isEmpty) {
      // empty list
      false
    } else if (nums.nonEmpty) {
      // found xxxxx-of-yyyyy pattern
      val parts = nums.map(_._1).sorted
      val total = nums.map(_._2).toSet
      metadata.size == nums.size &&  // all paths matched
        total.size == 1 && total.head == parts.size &&  // yyyyy part
        parts.head == 0 && parts.last + 1 == parts.size // xxxxx part
    } else {
      true
    }
  }

  private[scio] def getDirectoryInputStream(path: String,
                                            wrapperFn: InputStream => InputStream = identity)
  : InputStream = {
    val inputs = listFiles.map(getObjectInputStream).map(wrapperFn).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

}
