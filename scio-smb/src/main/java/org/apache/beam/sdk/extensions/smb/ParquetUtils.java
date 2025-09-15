/*
 * Copyright 2023 Spotify AB.
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

package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

class ParquetUtils {
  static <T, SELF extends ParquetWriter.Builder<T, SELF>> ParquetWriter<T> buildWriter(
      ParquetWriter.Builder<T, SELF> builder, Configuration conf, CompressionCodecName compression)
      throws IOException {
    // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
    long rowGroupSize =
        conf.getLong(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);

    for (Map.Entry<String, Boolean> entry :
        getColumnarConfig(conf, ParquetOutputFormat.BLOOM_FILTER_ENABLED, Boolean::parseBoolean)
            .entrySet()) {
      builder = builder.withBloomFilterEnabled(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Boolean> entry :
        getColumnarConfig(conf, ParquetOutputFormat.ENABLE_DICTIONARY, Boolean::parseBoolean)
            .entrySet()) {
      builder = builder.withDictionaryEncoding(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Long> entry :
        getColumnarConfig(conf, ParquetOutputFormat.BLOOM_FILTER_EXPECTED_NDV, Long::parseLong)
            .entrySet()) {
      builder = builder.withBloomFilterNDV(entry.getKey(), entry.getValue());
    }

    return builder
        .withConf(conf)
        .withCompressionCodec(compression)
        .withPageSize(ParquetOutputFormat.getPageSize(conf))
        .withPageRowCountLimit(
            conf.getInt(
                ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT,
                ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT))
        .withPageWriteChecksumEnabled(ParquetOutputFormat.getPageWriteChecksumEnabled(conf))
        .withWriterVersion(ParquetOutputFormat.getWriterVersion(conf))
        .withBloomFilterEnabled(ParquetOutputFormat.getBloomFilterEnabled(conf))
        .withDictionaryEncoding(ParquetOutputFormat.getEnableDictionary(conf))
        .withDictionaryPageSize(ParquetOutputFormat.getDictionaryPageSize(conf))
        .withMaxRowCountForPageSizeCheck(ParquetOutputFormat.getMaxRowCountForPageSizeCheck(conf))
        .withMinRowCountForPageSizeCheck(ParquetOutputFormat.getMinRowCountForPageSizeCheck(conf))
        .withValidation(ParquetOutputFormat.getValidation(conf))
        .withRowGroupSize(rowGroupSize)
        .withRowGroupSize(rowGroupSize)
        .build();
  }

  private static <T> Map<String, T> getColumnarConfig(
      Configuration conf, String key, Function<String, T> toT) {
    final String keyPrefix = key + "#";
    return conf.getPropsWithPrefix(keyPrefix).entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().replaceFirst(keyPrefix, ""),
                entry -> toT.apply(entry.getValue())));
  }
}
