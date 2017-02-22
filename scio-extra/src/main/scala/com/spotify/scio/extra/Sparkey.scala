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
import java.util.UUID

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.ScioContext
import com.spotify.scio.util.{CallSites, ScioUtil}
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.{CompressionType, SparkeyReader, Sparkey => JSparkey}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.apache.beam.sdk.values.PCollectionView

object Sparkey {

  /**
   * Represents the base URI for a Sparkey index and log file, either on the local file
   * system or on GCS. For GCS, the URI should be in the form
   * 'gs://<bucket>/<path>/<sparkey-prefix>'. For local files, the URI should be in the form
   * '/<path>/<sparkey-prefix>'. Note that the URI must not be a folder or GCS bucket as the URI is
   * a base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
   */
  trait SparkeyUri {
    val basePath: String
    def getReader(): SparkeyReader
    override def toString(): String = basePath
  }

  object SparkeyUri {
    def apply(path: String): SparkeyUri =
      if (ScioUtil.isGcsUri(new URI(path))) new GcsSparkeyUri(path) else new LocalSparkeyUri(path)
  }

  implicit class SparkeyScioContext(val self: ScioContext) {
    def sparkeySideInput(url: SparkeyUri): SideInput[SparkeyReader] = {
      val view = self.parallelize(Seq(url)).applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class SparkeyPairSCollection(val self: SCollection[(String, String)]) {
    /**
     * Write the contents of this SCollection as a Sparkey file, either locally or on GCS.
     *
     * @return A singleton SCollection containing the [[SparkeyUri]] of the saved files.
     */
    def asSparkey(uri: SparkeyUri): SCollection[SparkeyUri] = self.transform { in =>
      in.groupBy(_ => ())
        .map { case (_, iter) =>
          val writer = new SparkeyWriter(uri)
          val it = iter.iterator
          while (it.hasNext) {
            val kv = it.next()
            writer.put(kv._1.toString, kv._2.toString)
          }
          writer.close()
          uri
        }
    }

    /** Write the contents of this SCollection as a Sparkey file using the default uri. */
    def asSparkey: SCollection[SparkeyUri] = {
      val uid = UUID.randomUUID()
      val uri = ScioUtil.tempLocation(self.context.options) + s"/sparkey-$uid"
      this.asSparkey(SparkeyUri(uri))
    }

    def asSparkeySideInput: SideInput[SparkeyReader] = self.asSparkey.asSparkeySideInput
  }

  implicit class SparkeySCollection(val self: SCollection[SparkeyUri]) {
    def asSparkeySideInput: SideInput[SparkeyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class SparkeyPairSCollectionFunctions(val self: SCollection[(String, String)]) {
    def asSparkeySideInput: SideInput[SparkeyReader] = self.asSparkey.asSparkeySideInput
  }

  implicit class RichSparkeyReader(val self: SparkeyReader) extends Map[String, String] {
    override def get(key: String): Option[String] = Option(self.getAsString(key))

    override def iterator: Iterator[(String, String)] = new Iterator[(String, String)] {
      private val delegate = self.iterator()

      override def hasNext: Boolean = delegate.hasNext

      override def next(): (String, String) = {
        val entry = delegate.next()
        (entry.getKeyAsString, entry.getValueAsString)
      }
    }
    //scalastyle:off method.name
    override def +[B1 >: String](kv: (String, B1)): Map[String, B1] = ???
    override def -(key: String): Map[String, String] = ???
    //scalastyle:on method.name
  }

  private[scio] class SparkeySideInput(val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      SparkeyUri(context.sideInput(view).basePath).getReader()
  }

  private case class LocalSparkeyUri(basePath: String) extends SparkeyUri {
    override def getReader(): SparkeyReader = JSparkey.open(new File(basePath))
  }

  private object GcsSparkeyUri

  private case class GcsSparkeyUri(basePath: String) extends SparkeyUri {
    val localBasePath: String =
      // Hash the URI as part of the prefix to allow multiple Sparkey files per job
      sys.props("java.io.tmpdir") + "/" + hashPrefix(basePath)

    override def getReader(): SparkeyReader = {
      for (ext <- Seq("spi", "spl")) {
        val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
        ScioUtil.fetchFromGCS(gcs, new URI(s"$basePath.$ext"), s"$localBasePath.$ext")
      }
      JSparkey.open(new File(s"$localBasePath.spi"))
    }
  }

  private class SparkeyWriter(val uri: SparkeyUri) {
    private lazy val localFile = uri match {
      case LocalSparkeyUri(_) => uri.toString()
      case gcsUri: GcsSparkeyUri => gcsUri.localBasePath
    }

    private lazy val delegate = JSparkey.createNew(new File(localFile), CompressionType.NONE, 512)

    def put(key: String, value: String): Unit = delegate.put(key, value)

    def close(): Unit = {
      delegate.flush()
      delegate.writeHash()
      delegate.close()
      uri match {
        case GcsSparkeyUri(path) => {
          val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
          // Copy .spi and .spl to GCS path
          for (ext <- Seq("spi", "spl")) {
            val writer = gcs.create(GcsPath.fromUri(s"$path.$ext"), "application/octet-stream")
            writer.write(ByteBuffer.wrap(JFiles.readAllBytes(Paths.get(s"$localFile.$ext"))))
            writer.close()
          }
        }
        case _ => ()
      }
    }
  }

  private def hashPrefix(path: String): String =
    Hashing.sha1().hashString(path, Charsets.UTF_8).toString.substring(0, 8) + "-sparkey"
}
