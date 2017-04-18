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

package com.spotify.scio

import java.io._
import java.net.URI
import java.nio.channels.Channels
import java.security.PrivilegedAction
import java.util.Collections

import com.google.api.client.util.ByteStreams
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.io.{HdfsFileStorage, Tap, Taps}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.{DistCache, SCollection}
import com.twitter.chill.Externalizer
import org.apache.avro.Schema
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.hdfs._
import org.apache.beam.sdk.io.{Read, Write}
import org.apache.beam.sdk.options.GcsOptions
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Main package for HDFS APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.hdfs._
 * }}}
 *
 * Note that Hadoop configuration files, e.g. `core-site.xml`, `hdfs-site.xml`, must be present.
 * They can be packaged in src/main/resources directory.
 */
package object hdfs {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Enhanced version of [[ScioContext]] with HDFS methods. */
  implicit class HdfsScioContext(val self: ScioContext) extends AnyVal {

    /** Get an SCollection for a text file on HDFS. */
    def hdfsTextFile(path: String,
                     username: String = null,
                     conf: Configuration = null): SCollection[String] =
    self.requireNotClosed {
      val _conf = Option(conf).getOrElse(new Configuration())
      val src = HDFSFileSource.fromText(path).withConfiguration(_conf).withUsername(username)
      self.wrap(self.applyInternal(Read.from(src)))
    }

    /**
     * Get an SCollection for an Avro file on HDFS.
     * @param schema must be not null if `T` is of type
     *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
     */
    def hdfsAvroFile[T: ClassTag](path: String,
                                  schema: Schema = null,
                                  username: String = null,
                                  conf: Configuration = null): SCollection[T] =
    self.requireNotClosed {
      val _conf = Option(conf).getOrElse(new Configuration())
      val coder: AvroCoder[T] = if (schema == null) {
        AvroCoder.of(ScioUtil.classOf[T])
      } else {
        AvroCoder.of(ScioUtil.classOf[T], schema)
      }
      val src = HDFSFileSource.fromAvro(path, coder, _conf).withUsername(username)
      self.wrap(self.applyInternal(Read.from(src))).setName(path)
    }

    /**
     * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance for a file on HDFS.
     *
     * @param path to the HDFS artifact
     * @param conf optional custom Hadoop configuration
     * @param username optional Hadoop Simple Authentication remote username
     */
    def hadoopDistCache[F](path: String,
                           conf: Configuration = null,
                           username: String = null)
                          (initFn: File => F): DistCache[F] =
      hadoopDistCacheMulti(Seq(path), conf, username) {fs: Seq[File] => initFn(fs.head)}

    /**
     * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance for a list of
     * files on HDFS.
     *
     * @param paths Sequence of paths to the HDFS artifacts
     * @param conf optional custom Hadoop configuration
     * @param username optional Hadoop Simple Authentication remote username
     */
    def hadoopDistCacheMulti[F](paths: Seq[String],
                                conf: Configuration = null,
                                username: String = null)
                               (initFn: Seq[File] => F): DistCache[F] = self.requireNotClosed {
      if (self.isTest) {
        self.distCache(paths)(initFn)
      }
      else {
        require(!paths.contains(null), "Artifact path can't be null")

        val (targetDir, writeFn) = getTargetFromOptions
        val _conf = Option(conf).getOrElse(new Configuration())

        val targetPaths = paths.map { path =>
          //TODO: should we add checksums on both src and GCS to reuse uploaded artifacts?
          // to keep it simple, for now we upload each artifact, thus artifacts should be
          // relatively small.
          val pathHash = Hashing.sha1().hashString(path, Charsets.UTF_8)
          val targetHash = pathHash.toString.substring(0, 8)
          logger.debug(s"Add '$path' (hash: '$pathHash') to dist cache")

          val targetDistCache = new Path("distcache", s"$targetHash-${path.split("/").last}")

          val target = new Path(targetDir, targetDistCache)

          // TODO: should upload be asynchronous, blocking on context close?
          if (username != null) {
            UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedAction[Unit] {
              override def run(): Unit = {
                hadoopDistCacheCopy(new Path(path), target.toUri, _conf)(writeFn)
              }
            })
          } else {
            hadoopDistCacheCopy(new Path(path), target.toUri, _conf)(writeFn)
          }

          target
        }

        self.distCache(targetPaths.map(_.toString))(initFn)
      }
    }

    private def getTargetFromOptions: (String, URI => OutputStream) = {
      val writeFn = if (ScioUtil.isLocalRunner(self.options)) {
        localOutputStream _
      } else {
        gcsOutputStream _
      }
      (self.options.getTempLocation, writeFn)
    }

    private def localOutputStream(target: URI): OutputStream = {
      val f = new File(target.getPath)
      f.getParentFile.mkdir()
      new FileOutputStream(f)
    }

    private def gcsOutputStream(target: URI): OutputStream = {
      //TODO: Should we attempt to detect the Mime type rather than always using MimeTypes.BINARY?
      val gcsOptions = self.optionsAs[GcsOptions]
      Channels.newOutputStream(
        gcsOptions.getGcsUtil.create(GcsPath.fromUri(target), MimeTypes.BINARY))
    }

    private[scio] def hadoopDistCacheCopy(src: Path, target: URI, conf: Configuration)
                                         (createOutputStream: URI => OutputStream): Unit = {
      logger.debug(s"Will copy ${src.toUri}, to $target")

      val fs = src.getFileSystem(conf)
      val inStream = fs.open(src)
      val outChannel = createOutputStream(target)

      try {
        ByteStreams.copy(inStream, outChannel)
      } finally {
        outChannel.close()
        inStream.close()
      }
    }

  }

  // Cannot extend AnyVal due to ClassTag
  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with HDFS methods. */
  implicit class HdfsSCollection[T: ClassTag](val self: SCollection[T]) {

    /** Save this SCollection as a text file on HDFS. */
    // TODO: numShards
    def saveAsHdfsTextFile(path: String,
                           username: String = null,
                           conf: Configuration = null): Future[Tap[String]] = {
      val _conf = Option(conf).getOrElse {
        val newConf = new Configuration()
        // Writing to remote HDFS might be slow without compression.
        // Deflate level can be between [1-9], higher means better compression but lower speed
        // and 6 is a good compromise.
        newConf.setBoolean(FileOutputFormat.COMPRESS, true)
        newConf.set(FileOutputFormat.COMPRESS_CODEC, classOf[DefaultCodec].getName)
        newConf.set(FileOutputFormat.COMPRESS_TYPE, "BLOCK")
        newConf
      }

      val sink = HDFSFileSink.toText[T](path).withConfiguration(_conf).withUsername(username)
      self.applyInternal(Write.to(sink))

      self.context.makeFuture(HdfsTextTap(path))
    }

    /**
     * Save this SCollection as an Avro file.
     * @param schema must be not null if `T` is of type
     *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
     */
    // TODO: numShards
    def saveAsHdfsAvroFile(path: String,
                           schema: Schema = null,
                           username: String = null,
                           conf: Configuration = null): Future[Tap[T]] =
    {
      val _conf = Option(conf).getOrElse{
        val newConf = new Configuration()
        // Writing to remote HDFS might be slow without compression.
        // Deflate level can be between [1-9], higher means better compression but lower speed
        // and 6 is a good compromise.
        newConf.setBoolean(FileOutputFormat.COMPRESS, true)
        newConf.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, 6)
        newConf
      }

      val s = if (schema == null) {
        ScioUtil.classOf[T].getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
      } else {
        schema
      }
      val sink = HDFSFileSink.toAvro[T](path, AvroCoder.of(ScioUtil.classOf[T], s), _conf)
        .withUsername(username)
      self.applyInternal(Write.to(sink))
      self.context.makeFuture(HdfsAvroTap[T](path, schema))
    }
  }

  /** Enhanced version of [[com.spotify.scio.io.Taps Taps]] with HDFS methods. */
  implicit class HdfsTaps(val self: Taps) extends AnyVal {

    /** Get a `Future[Tap[T]]` for a text file on HDFS. */
    def hdfsTextFile(path: String): Future[Tap[String]] =
      self.mkTap(s"Text: $path", () => isPathDone(path), () => HdfsTextTap(path))

    /** Get a `Future[Tap[T]]` for a Avro file on HDFS. */
    def hdfsAvroFile[T: ClassTag](path: String, schema: Schema = null): Future[Tap[T]] =
      self.mkTap(s"Avro: $path", () => isPathDone(path), () => HdfsAvroTap(path, schema))

    private def isPathDone(path: String): Boolean = {
      val conf = new Configuration()
      val fs = FileSystem.get(new URI(path), conf)
      fs.exists(new Path(path, "_SUCCESS"))
    }

  }

  /** Tap for text files on HDFS. */
  case class HdfsTextTap(path: String) extends Tap[String] {
    override def value: Iterator[String] = HdfsFileStorage(path).textFile
    override def open(sc: ScioContext): SCollection[String] =
      sc.hdfsTextFile(ScioUtil.addPartSuffix(path))
  }

  /** Tap for Avro files on HDFS. */
  case class HdfsAvroTap[T: ClassTag](path: String,
                                      @transient private val schema: Schema = null) extends Tap[T] {
    private lazy val s = Externalizer(schema)
    override def value: Iterator[T] = HdfsFileStorage(path).avroFile()
    override def open(sc: ScioContext): SCollection[T] = sc.hdfsAvroFile[T](path, s.get)
  }

  private object HdfsUtil {

    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean =
        !path.getName.startsWith("_") && !path.getName.startsWith(".")
    }

    def getDirectoryInputStream(path: String): InputStream = {
      val conf = new Configuration()
      val fs = FileSystem.get(new URI(path), conf)
      val streams = fs
        .listStatus(new Path(path), pathFilter)
        .map { status =>
          fs.open(status.getPath)
        }
      new SequenceInputStream(Collections.enumeration(streams.toList.asJava))
    }

  }

}
