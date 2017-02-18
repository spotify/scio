/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{Paths, Files => JFiles}

import com.google.common.io.Files
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.{CompressionType, SparkeyReader, SparkeyWriter, Sparkey => JSparkey}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.apache.beam.sdk.values.PCollectionView

import scala.collection.JavaConverters._
import scala.util.Try

object Sparkey {

  implicit class SparkeyScioContext(val self: ScioContext) {
    def sparkeySideInput(url: SparkeyUri): SideInput[SparkeyReader] = {
      val view = self.parallelize(Seq(url)).applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class SCollectionWithSparkeyWriter[K, V](val self: SCollection[(K, V)])
                                                 (implicit ev1: K <:< String, ev2: V <:< String) {
    def asSparkey(uri: SparkeyUri): SCollection[SparkeyUri] = self.transform { in =>
      in.groupBy(_ => ())
        .map { case (_, iter) =>
          val writer = uri.getWriter()
          val it = iter.iterator
          while (it.hasNext) {
            val kv = it.next()
            writer.put(kv._1.toString, kv._2.toString)
          }
          writer.close()
          uri
        }
    }

    def asSparkey: SCollection[SparkeyUri] = {
      val tempLocation = Try(self.context.optionsAs[DataflowPipelineOptions].getGcpTempLocation)
        .getOrElse(self.context.options.getTempLocation)
      this.asSparkey(SparkeyUri(tempLocation))
    }
  }

  implicit class SparkeySCollection(val self: SCollection[SparkeyUri]) {
    def asSparkeySideInput(): SideInput[SparkeyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class SCollectionToSparkeySideInput(val self: SCollection[(String, String)]) {
    def asSparkeySideInput(): SideInput[SparkeyReader] = self.asSparkey.asSparkeySideInput()
  }

  implicit class Reader(val self: SparkeyReader) {
    def get(key: String): Option[String] = Option(self.getAsString(key))

    def toStream(): Stream[(String, String)] = self.iterator().asScala.toStream.map { e =>
      (e.getKeyAsString, e.getValueAsString)
    }
  }

  private[scio] class SparkeySideInput(val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      SparkeyUri(context.sideInput(view).path).getReader()
  }
}

trait SparkeyUri extends Serializable {
  val path: String
  def getWriter(): WrappedSparkeyWriter
  def getReader(): SparkeyReader
}

object SparkeyUri {
  def apply(path: String): SparkeyUri =
    if (new URI(path).getScheme == "gs") new GcsSparkeyUri(path) else new LocalSparkeyUri(path)
}

private case class LocalSparkeyUri(path: String) extends SparkeyUri {
  override def getReader(): SparkeyReader = JSparkey.open(new File(path + ".spi"))

  override def getWriter(): WrappedSparkeyWriter = new LocalSparkeyWriter(path)
}

private case class GcsSparkeyUri(path: String) extends SparkeyUri {
  override def getReader(): SparkeyReader = {
    val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
    val localTmpDir = Files.createTempDir()
    // Copy .spi and .spl to local files
    for (ext <- Seq(".spi", ".spl")) {
      val src = gcs.open(GcsPath.fromUri(s"$path/sparkey$ext"))
      val dst = new FileOutputStream(s"$localTmpDir/sparkey$ext").getChannel
      dst.transferFrom(src, 0, src.size())
      src.close()
      dst.close()
    }
    JSparkey.open(new File(localTmpDir + "/sparkey.spi"))
  }

  override def getWriter(): WrappedSparkeyWriter = new GcsSparkeyWriter(path)
}

trait WrappedSparkeyWriter {
  protected val delegate: SparkeyWriter

  val path: String

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    delegate.writeHash()
    delegate.close()
  }
}

private class LocalSparkeyWriter(val path: String) extends WrappedSparkeyWriter {
  val delegate = JSparkey.createNew(new File(path + ".spi"), CompressionType.NONE, 512)
}

private class GcsSparkeyWriter(val path: String) extends WrappedSparkeyWriter {
  private val localTmpDir = Files.createTempDir()
  private val localIndex = localTmpDir + "/sparkey.spi"

  val delegate = JSparkey.createNew(new File(localIndex), CompressionType.NONE, 512)

  override def close(): Unit = {
    super.close()
    val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
    // Copy .spi and .spl to GCS path
    for (ext <- Seq(".spi", ".spl")) {
      val writer = gcs.create(GcsPath.fromUri(s"$path/sparkey$ext"), "application/octet-stream")
      writer.write(ByteBuffer.wrap(JFiles.readAllBytes(Paths.get(localIndex.replace(".spi", ext)))))
      writer.close()
    }
  }
}
