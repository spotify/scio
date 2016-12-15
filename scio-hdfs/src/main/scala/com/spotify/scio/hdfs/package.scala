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
import java.nio.file.Files
import java.security.PrivilegedAction
import java.util.Collections

import com.google.api.client.util.ByteStreams
import com.google.cloud.dataflow.sdk.io.hdfs._
import com.google.cloud.dataflow.sdk.io.hdfs.simpleauth._
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.{DistCache, SCollection}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.mapred.{AvroKey, AvroOutputFormat}
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.{Read, Write}
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.util.gcsfs.GcsPath
import org.apache.beam.sdk.values.KV
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
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

  /** Enhanced version of [[ScioContext]] with HDFS methods. */
  // TODO: scala 2.11
  // implicit class HdfsScioContext(private val sc: ScioContext) extends AnyVal {
  implicit class HdfsScioContext(val self: ScioContext) {
    private val logger = LoggerFactory.getLogger(ScioContext.getClass)

    /** Get an SCollection for a text file on HDFS. */
    def hdfsTextFile(path: String, username: String = null)
    : SCollection[String] = self.requireNotClosed {
      val src = if (username != null) {
        SimpleAuthHDFSFileSource.from(
          path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], username)
      } else {
        HDFSFileSource.from(
          path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      }
      self.wrap(self.applyInternal(Read.from(src)))
        .setName(path)
        .map(_.getValue.toString)
    }

    /** Get an SCollection of specific record type for an Avro file on HDFS. */
    def hdfsAvroFile[T: ClassTag](path: String,
                                  schema: Schema = null,
                                  username: String = null): SCollection[T] = self.requireNotClosed {
      val coder: AvroCoder[T] = if (schema == null) {
        AvroCoder.of(ScioUtil.classOf[T])
      } else {
        AvroCoder.of(ScioUtil.classOf[T], schema)
      }
      val src = if (username != null) {
        new SimpleAuthAvroHDFSFileSource[T](path, coder, username)
      } else {
        new AvroHDFSFileSource[T](path, coder)
      }

      self.wrap(self.applyInternal(Read.from(src)))
        .setName(path)
        .map(_.getKey.datum())
    }

    /**
     * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance for a file on
     * Hadoop/HDFS.
     *
     * @param path to the Hadoop/HDFS artifact
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
     * files on Hadoop/HDFS.
     *
     * @param paths Sequence of paths to the Hadoop/HDFS artifacts
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

        val (targetDir, writeFn) = getTargetFromOptions()
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

          // TODO: should upload be asynchronous, blocking on context close
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

    private def getTargetFromOptions() = {
      if (!ScioUtil.isLocalRunner(self.options)) {
        val dfOptions = self.optionsAs[DataflowPipelineOptions]
        require(self.optionsAs[DataflowPipelineOptions].getStagingLocation != null,
          "Staging directory not set - use `--stagingLocation`!")
        (dfOptions.getStagingLocation, gcsOutputStream _)
      } else {
        // should targetDir be specified in options?
        (Files.createTempDirectory("distcache").toString, localOutputStream _)
      }
    }

    private def localOutputStream(target: URI): OutputStream = {
      val f = new File(target.getPath)
      f.getParentFile.mkdir()
      new FileOutputStream(f)
    }

    private def gcsOutputStream(target: URI): OutputStream = {
      //TODO: Should we attempt to detect the Mime type rather than always using MimeTypes.BINARY?
      val dfOptions = self.optionsAs[DataflowPipelineOptions]
      Channels.newOutputStream(
        dfOptions.getGcsUtil.create(GcsPath.fromUri(target), MimeTypes.BINARY))
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

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with HDFS methods. */
  // TODO: scala 2.11
  // implicit class HdfsSCollection[T](private val self: SCollection[T]) extends AnyVal {
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
        newConf.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, 6)
        newConf
      }

      val sink = if (username != null) {
        new SimpleAuthHDFSFileSink(path,
                                     classOf[TextOutputFormat[NullWritable, Text]],
                                     _conf,
                                     username)
      } else {
        new HDFSFileSink(path, classOf[TextOutputFormat[NullWritable, Text]], _conf)
      }
      self
        .map(x => KV.of(NullWritable.get(), new Text(x.toString)))
        .applyInternal(Write.to(sink))
      self.context.makeFuture(HdfsTextTap(path))
    }

    /** Save this SCollection as an Avro file on HDFS. */
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

      val job = Job.getInstance(_conf)
      val jobConf = job.getConfiguration
      val s = if (schema == null) {
        ScioUtil.classOf[T].getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
      } else {
        schema
      }
      AvroJob.setOutputKeySchema(job, s)
      val sink = if (username != null) {
        new SimpleAuthHDFSFileSink(path, classOf[AvroKeyOutputFormat[T]], jobConf, username)
      } else {
        new HDFSFileSink(path, classOf[AvroKeyOutputFormat[T]], jobConf)
      }
      self
        .map(x => KV.of(new AvroKey(x), NullWritable.get()))
        .applyInternal(Write.to(sink))
      self.context.makeFuture(HdfsAvroTap[T](path, schema))
    }
  }

  /** Enhanced version of [[com.spotify.scio.io.Taps]] with HDFS methods. */
  // TODO: scala 2.11
  // implicit class HdfsTaps(private val self: Taps) extends AnyVal {
  implicit class HdfsTaps(val self: Taps) {

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

    override def open(sc: ScioContext): SCollection[String] = sc.hdfsTextFile(path + "/part-*")

  }

  /** Tap for Avro files on HDFS. */
  case class HdfsAvroTap[T: ClassTag](path: String, schema: Schema = null) extends Tap[T] {

    override def value: Iterator[T] = HdfsFileStorage(path).avroFile()

    override def open(sc: ScioContext): SCollection[T] = sc.hdfsAvroFile[T](path, schema)

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
