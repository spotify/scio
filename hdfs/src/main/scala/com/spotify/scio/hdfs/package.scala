package com.spotify.scio

import com.google.cloud.dataflow.contrib.hadoop.{AvroHadoopFileSource, HadoopFileSink, HadoopFileSource}
import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.io.{Read, Write}
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

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
  implicit class HdfsScioContext(val sc: ScioContext) {

    /** Get an SCollection for a text file on HDFS. */
    def hdfsTextFile(path: String): SCollection[String] = {
      val src = HadoopFileSource.from(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      sc.wrap(sc.applyInternal(Read.from(src)))
        .map(_.getValue.toString)
    }

    /** Get an SCollection of specific record type for an Avro file on HDFS. */
    def hdfsAvroFile[T: ClassTag](path: String, schema: Schema = null): SCollection[T] = {
      val coder: AvroCoder[T] = if (schema == null) {
        AvroCoder.of(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
      } else {
        AvroCoder.of(schema).asInstanceOf[AvroCoder[T]]
      }
      val src = new AvroHadoopFileSource[T](path, coder)
      sc.wrap(sc.applyInternal(Read.from(src)))
        .map(_.getKey.datum())
    }

  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with HDFS methods. */
  // TODO: scala 2.11
  // implicit class HdfsSCollection[T](private val self: SCollection[T]) extends AnyVal {
  implicit class HdfsSCollection[T: ClassTag](val self: SCollection[T]) {

    /** Save this SCollection as a text file on HDFS. Note that elements must be of type String. */
    // TODO: Future[Tap[T]]
    // TODO: numShards
    def saveAsHdfsTextFile(path: String)(implicit ev: T <:< String): Unit =
      self
        .map(x => KV.of(NullWritable.get(), new Text(x.asInstanceOf[String])))
        .applyInternal(Write.to(new HadoopFileSink(path, classOf[TextOutputFormat[NullWritable, Text]])))

    /** Save this SCollection as an Avro file on HDFS. Note that elements must be of type IndexedRecord. */
    // TODO: Future[Tap[T]]
    // TODO: numShards
    def saveAsHdfsAvroFile(path: String, schema: Schema = null)(implicit ev: T <:< IndexedRecord): Unit = {
      val job = Job.getInstance()
      if (schema != null) {
        AvroJob.setOutputKeySchema(job, schema)
      } else {
        val s = implicitly[ClassTag[T]].runtimeClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
        AvroJob.setOutputKeySchema(job, s)
      }
      self
        .map(x => KV.of(new AvroKey(x), NullWritable.get()))
        .applyInternal(Write.to(new HadoopFileSink(path, classOf[AvroKeyOutputFormat[T]], job.getConfiguration)))
    }

  }

}
