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
import java.nio.channels.Channels
import java.nio.file.Path
import java.util.Collections

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.util.Charsets
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private object FileStorage {
  def apply(path: String): FileStorage =
    if (new URI(path).getScheme == "gs") new GcsStorage(path) else new LocalStorage(path)
}

private trait FileStorage {

  protected val path: String

  protected def list: Seq[Path]

  protected def getObjectInputStream(path: Path): InputStream

  def avroFile[T: ClassTag](schema: Schema = null): Iterator[T] = {
    val cls = ScioUtil.classOf[T]
    val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      new SpecificDatumReader[T](cls)
    } else {
      new GenericDatumReader[T](schema)
    }
    new DataFileStream[T](getDirectoryInputStream(path), reader).iterator().asScala
  }

  def textFile: Iterator[String] =
    IOUtils.lineIterator(getDirectoryInputStream(path), Charsets.UTF_8).asScala

  def tableRowJsonFile: Iterator[TableRow] = {
    val mapper = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    textFile.map(mapper.readValue(_, classOf[TableRow]))
  }

  private def getDirectoryInputStream(path: String): InputStream = {
    val inputs = list.map(getObjectInputStream).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

}

private class GcsStorage(protected val path: String) extends FileStorage {

  private val uri = new URI(path)
  require(ScioUtil.isGcsUri(uri), s"Not a GCS path: $path")

  private lazy val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())

  override protected def list: Seq[Path] = gcs.expand(GcsPath.fromUri(uri)).asScala

  override protected def getObjectInputStream(path: Path): InputStream =
    Channels.newInputStream(gcs.open(GcsPath.fromUri(path.toUri)))

}

private class LocalStorage(protected val path: String)  extends FileStorage {

  private val uri = new URI(path)
  require(ScioUtil.isLocalUri(uri), s"Not a local path: $path")

  override protected def list: Seq[Path] = {
    val p = path.lastIndexOf("/")
    FileUtils
      .listFiles(new File(path.substring(0, p)), new WildcardFileFilter(path.substring(p + 1)), null)
      .asScala
      .toSeq
      .map(_.toPath)
  }

  override protected def getObjectInputStream(path: Path): InputStream = new FileInputStream(path.toFile)

}
