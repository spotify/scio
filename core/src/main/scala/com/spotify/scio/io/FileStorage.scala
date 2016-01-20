package com.spotify.scio.io

import java.io.{File, FileInputStream, InputStream, SequenceInputStream}
import java.net.URI
import java.util.Collections

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.Charsets
import com.google.api.services.storage.Storage
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.util.Util
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private object FileStorage {
  def apply(path: String): FileStorage =
    if (new URI(path).getScheme == "gs") new GcsStorage(path) else new LocalStorage(path)
}

private trait FileStorage {

  protected val path: String

  def delete(): Unit

  protected def list: Seq[String]

  protected def getObjectInputStream(path: String): InputStream

  def genericAvroFile: Iterator[GenericRecord] = {
    val reader = new GenericDatumReader[GenericRecord]()
    new DataFileStream[GenericRecord](getDirectoryInputStream(path), reader).iterator().asScala
  }

  def specificAvroFile[T: ClassTag]: Iterator[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val reader = new SpecificDatumReader[T](cls)
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
  require(Util.isGcsUri(uri), s"Not a GCS path: $path")

  lazy private val storage = new Storage.Builder(
    GoogleNetHttpTransport.newTrustedTransport(),
    JacksonFactory.getDefaultInstance,
    GoogleCredential.getApplicationDefault).build()

  override def delete(): Unit = {
    val bucket = uri.getHost
    val prefix = uri.getPath.replaceAll("^/", "") + (if (uri.getPath.endsWith("/")) "" else "/")
    val objects = storage.objects()
      .list(bucket)
      .setPrefix(prefix)
      .execute()
      .getItems.asScala
    objects.foreach { o =>
      storage.objects().delete(bucket, o.getName).execute()
    }
  }

  override protected def list: Seq[String] = {
    val bucket = uri.getHost
    val prefix = uri.getPath.replaceAll("^/", "") + (if (uri.getPath.endsWith("/")) "" else "/")
    val pDelimiters = prefix.count(isDelimiter)
    storage.objects()
      .list(bucket)
      .setPrefix(prefix)
      .execute()
      .getItems.asScala
      .filter { o =>
        val n = o.getName
        val nDelimiters: Int = n.count(isDelimiter)
        BigInt(o.getSize) > 0 && !isHidden(n) && nDelimiters == pDelimiters && n != prefix
      }
      .map(o => s"gs://$bucket/${o.getName}")
  }

  override protected def getObjectInputStream(path: String): InputStream = {
    val uri = new URI(path)
    val bucket = uri.getHost
    val obj = uri.getPath.replaceAll("^/", "")
    storage.objects().get(bucket, obj).executeMediaAsInputStream()
  }

  private def isDelimiter(char: Char) = char == '/'

  private def isHidden(path: String) = !path.endsWith("/") && Set('_', '.')(path.charAt(path.lastIndexOf('/') + 1))

}

private class LocalStorage(protected val path: String)  extends FileStorage {

  private val uri = new URI(path)
  require(Util.isLocalUri(uri), s"Not a local path: $path")

  override def delete(): Unit = FileUtils.deleteDirectory(new File(uri))

  override protected def list: Seq[String] =
    FileUtils.listFiles(new File(path), null, false).asScala.map(_.getCanonicalPath).toSeq

  override protected def getObjectInputStream(path: String): InputStream = new FileInputStream(path)

}
