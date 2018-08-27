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

package com.spotify.scio.parquet.avro.nio

import com.spotify.scio.parquet.avro._
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.io.Tap
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.util.ScioUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.reflect.ReflectData
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future

final case class ParquetAvroIO[T](path: String) extends ScioIO[T] {

  type ReadP = Nothing
  type WriteP = ParquetAvroIO.WriteParam

  override def id: String = path

  // FIXME: implement this
  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new IllegalStateException("ParquetAvroIO is read-only")

  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
      val job = Job.getInstance()
      if (ScioUtil.isLocalRunner(data.context.options.getRunner)) {
        GcsConnectorUtil.setCredentials(job)
      }

      val cls = data.ct.runtimeClass
      val writerSchema = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        ReflectData.get().getSchema(cls)
      } else {
        params.schema
      }
      val resource = FileBasedSink.convertToFileResourceIfPossible(data.pathWithShards(path))
      val prefix = StaticValueProvider.of(resource)
      val usedFilenamePolicy = DefaultFilenamePolicy.fromStandardParameters(
        prefix, null, "", false)
      val destinations = DynamicFileDestinations.constant[T](usedFilenamePolicy)
      val sink = new ParquetAvroSink[T](
        prefix, destinations, writerSchema, job.getConfiguration, params.compression)
      val t = HadoopWriteFiles.to(sink).withNumShards(params.numShards)
      data.applyInternal(t)
      Future.failed(new NotImplementedError("Parquet Avro future not implemented"))
  }

  override def tap(params: ReadP): Tap[T] =
    throw new NotImplementedError("Parquet Avro tap not implemented")
}

object ParquetAvroIO {
  final case class WriteParam(numShards: Int = 0,
                              schema: Schema = null,
                              suffix: String = "",
                              compression: CompressionCodecName = CompressionCodecName.SNAPPY)
}
