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

import java.io.{File, FileInputStream, InputStream, SequenceInputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Path
import java.util.Collections
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.util.Charsets
import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

private object FileStorage {
  def apply(path: String): FileStorage =
    if (new URI(path).getScheme == "gs") new GcsStorage(path) else new LocalStorage(path)
}

private trait FileStorage {

  protected val path: String

  protected def listFiles: Seq[Path]

  protected def getObjectInputStream(path: Path): InputStream

  protected def getAvroSeekableInput(path: Path): SeekableInput

  def avroFile[T: ClassTag](schema: Schema = null): Iterator[T] = {
    val cls = ScioUtil.classOf[T]
    val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      new SpecificDatumReader[T](cls)
    } else {
      new GenericDatumReader[T](schema)
    }

    listFiles.map(f => DataFileReader.openReader(getAvroSeekableInput(f), reader))
      .map(_.iterator().asScala).reduce(_ ++ _)
  }

  def textFile: Iterator[String] =
    IOUtils.lineIterator(getDirectoryInputStream(path), Charsets.UTF_8).asScala

  def tableRowJsonFile: Iterator[TableRow] = {
    val mapper = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    textFile.map(mapper.readValue(_, classOf[TableRow]))
  }

  def isDone: Boolean = {
    val partPattern = "([0-9]{5})-of-([0-9]{5})".r
    val paths = listFiles
    val nums = paths.flatMap { p =>
      val m = partPattern.findAllIn(p.toString)
      if (m.hasNext) {
        Some(m.group(1).toInt, m.group(2).toInt)
      } else {
        None
      }
    }

    if (paths.isEmpty) {
      // empty list
      false
    } else if (nums.nonEmpty) {
      // found xxxxx-of-yyyyy pattern
      val parts = nums.map(_._1).sorted
      val total = nums.map(_._2).toSet
      paths.size == nums.size &&  // all paths matched
        total.size == 1 && total.head == parts.size &&  // yyyyy part
        parts.head == 0 && parts.last + 1 == parts.size // xxxxx part
    } else {
      true
    }
  }

  private def getDirectoryInputStream(path: String): InputStream = {
    val inputs = listFiles.map(getObjectInputStream).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

}

private class GcsStorage(protected val path: String) extends FileStorage {

  private val uri = new URI(path)
  require(ScioUtil.isGcsUri(uri), s"Not a GCS path: $path")

  private lazy val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())

  private val GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*")

  override protected def listFiles: Seq[Path] = {
    if (GLOB_PREFIX.matcher(path).matches()) {
      gcs
        .expand(GcsPath.fromUri(uri))
        .asScala
    } else {
      // not a glob, GcsUtil may return non-existent files
      val p = GcsPath.fromUri(path)
      if (Try(gcs.fileSize(p)).isSuccess) Seq(p) else Seq.empty
    }
  }

  override protected def getObjectInputStream(path: Path): InputStream =
    Channels.newInputStream(gcs.open(GcsPath.fromUri(path.toUri)))

  override protected def getAvroSeekableInput(path: Path): SeekableInput =
    new SeekableInput {
      private val in = gcs.open(GcsPath.fromUri(path.toUri))

      override def tell(): Long = in.position()

      override def length(): Long = in.size()

      override def seek(p: Long): Unit = in.position(p)

      override def read(b: Array[Byte], off: Int, len: Int): Int =
        in.read(ByteBuffer.wrap(b, off, len))

      override def close(): Unit = in.close()
    }

}

private class LocalStorage(protected val path: String)  extends FileStorage {

  private val uri = new URI(path)
  require(ScioUtil.isLocalUri(uri), s"Not a local path: $path")

  override protected def listFiles: Seq[Path] = {
    val p = path.lastIndexOf("/")
    val (dir, filter) = if (p == 0) {
      // "/file.ext"
      (new File("/"), new WildcardFileFilter(path.substring(p + 1)))
    } else if (p > 0) {
      // "/path/to/file.ext"
      (new File(path.substring(0, p)), new WildcardFileFilter(path.substring(p + 1)))
    } else {
      // "file.ext"
      (new File("."), new WildcardFileFilter(path))
    }
    FileUtils
      .listFiles(dir, filter, null)
      .asScala
      .toSeq
      .map(_.toPath)
  }

  override protected def getObjectInputStream(path: Path): InputStream =
    new FileInputStream(path.toFile)

  override protected def getAvroSeekableInput(path: Path): SeekableInput =
    new SeekableFileInput(path.toFile)

}
