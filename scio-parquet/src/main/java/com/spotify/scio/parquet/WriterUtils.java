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

package com.spotify.scio.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class WriterUtils {
  public static <T, SELF extends ParquetWriter.Builder<T, SELF>> ParquetWriter<T> build(
      ParquetWriter.Builder<T, SELF> builder, Configuration conf, CompressionCodecName compression)
      throws IOException {
    // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
    int rowGroupSize =
        conf.getInt(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
    return builder
        .withConf(conf)
        .withCompressionCodec(compression)
        .withRowGroupSize(rowGroupSize)
        .build();
  }
}
