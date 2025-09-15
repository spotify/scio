/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.parquet

import com.spotify.scio.parquet.types.syntax.Syntax
import magnolify.parquet.ParquetType
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.channels.WritableByteChannel

/**
 * Main package for Parquet type-safe APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.types._
 * }}}
 */
package object types extends Syntax {
  private[scio] case class ParquetTypeSink[T](
    compression: CompressionCodecName,
    conf: SerializableConfiguration
  )(implicit val pt: ParquetType[T])
      extends FileIO.Sink[T] {
    @transient private var writer: ParquetWriter[T] = _

    override def open(channel: WritableByteChannel): Unit = {
      // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
      val rowGroupSize =
        conf.get().getLong(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
      writer = pt
        .writeBuilder(new ParquetOutputFile(channel))
        .withCompressionCodec(compression)
        .withConf(conf.get())
        .withRowGroupSize(rowGroupSize)
        .build()
    }

    override def write(element: T): Unit = writer.write(element)
    override def flush(): Unit = writer.close()
  }
}
