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

import java.io.{InputStream, SequenceInputStream}
import java.util.Collections

import com.google.cloud.dataflow.contrib.hadoop._
import com.google.cloud.dataflow.contrib.hadoop.simpleauth._
import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.io.{Read, Write}
import com.google.cloud.dataflow.sdk.values.KV
import com.google.common.base.Charsets
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.mapred.{AvroKey, AvroOutputFormat}
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, DefaultCodec, DeflateCodec}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, MRConfig, MRJobConfig}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

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

    /** Get an SCollection for a text file on HDFS. */
    def hdfsTextFile(path: String, username: String = null): SCollection[String] = self.pipelineOp {
      val src = if (username != null) {
        SimpleAuthHadoopFileSource.from(
          path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], username)
      } else {
        HadoopFileSource.from(
          path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      }
      self.wrap(self.applyInternal(Read.from(src)))
        .setName(path)
        .map(_.getValue.toString)
    }

    /** Get an SCollection of specific record type for an Avro file on HDFS. */
    def hdfsAvroFile[T: ClassTag](path: String,
                                  schema: Schema = null,
                                  username: String = null): SCollection[T] = self.pipelineOp {
      val coder: AvroCoder[T] = if (schema == null) {
        AvroCoder.of(ScioUtil.classOf[T])
      } else {
        AvroCoder.of(schema).asInstanceOf[AvroCoder[T]]
      }
      val src = if (username != null) {
        new SimpleAuthAvroHadoopFileSource[T](path, coder, username)
      } else {
        new AvroHadoopFileSource[T](path, coder)
      }

      self.wrap(self.applyInternal(Read.from(src)))
        .setName(path)
        .map(_.getKey.datum())
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
        new SimpleAuthHadoopFileSink(path,
                                     classOf[TextOutputFormat[NullWritable, Text]],
                                     _conf,
                                     username)
      } else {
        new HadoopFileSink(path, classOf[TextOutputFormat[NullWritable, Text]], _conf)
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
        new SimpleAuthHadoopFileSink(path, classOf[AvroKeyOutputFormat[T]], jobConf, username)
      } else {
        new HadoopFileSink(path, classOf[AvroKeyOutputFormat[T]], jobConf)
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
      val fs = FileSystem.get(conf)
      fs.exists(new Path(path, "_SUCCESS"))
    }

  }

  /** Tap for text files on HDFS. */
  case class HdfsTextTap(path: String) extends Tap[String] {
    override def value: Iterator[String] = {
      val conf = new Configuration()
      val factory = new CompressionCodecFactory(conf)
      val fs = FileSystem.get(conf)
      val streams = fs
        .listStatus(new Path(path), HdfsUtil.pathFilter)
        .map { status =>
          val p = status.getPath
          val codec = factory.getCodec(p)
          if (codec != null) {
            codec.createInputStream(fs.open(p))
          } else {
            fs.open(p)
          }
        }
      val stream = new SequenceInputStream(Collections.enumeration(streams.toList.asJava))
      IOUtils.lineIterator(stream, Charsets.UTF_8).asScala
    }
    override def open(sc: ScioContext): SCollection[String] = sc.hdfsTextFile(path + "/part-*")
  }

  /** Tap for Avro files on HDFS. */
  case class HdfsAvroTap[T: ClassTag](path: String, schema: Schema = null) extends Tap[T] {
    override def value: Iterator[T] = {
      val cls = ScioUtil.classOf[T]
      val stream = HdfsUtil.getDirectoryInputStream(path)
      val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        new SpecificDatumReader[T](cls)
      } else {
        new GenericDatumReader[T](schema)
      }
      new DataFileStream[T](stream, reader).iterator().asScala
    }
    override def open(sc: ScioContext): SCollection[T] = sc.hdfsAvroFile[T](path, schema)
  }

  private object HdfsUtil {

    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean =
        !path.getName.startsWith("_") && !path.getName.startsWith(".")
    }

    def getDirectoryInputStream(path: String): InputStream = {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val streams = fs
        .listStatus(new Path(path), pathFilter)
        .map { status =>
          fs.open(status.getPath)
        }
      new SequenceInputStream(Collections.enumeration(streams.toList.asJava))
    }

  }

}
