/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.parquet.tensorflow;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class TensorflowExampleParquetWriter extends ParquetWriter<Example> {

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  TensorflowExampleParquetWriter(
      Path file,
      WriteSupport<Example> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      boolean enableDictionary,
      boolean enableValidation,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    super(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        pageSize,
        enableDictionary,
        enableValidation,
        writerVersion,
        conf);
  }

  private static WriteSupport<Example> writeSupport(Configuration conf, Schema schema) {
    return new TensorflowExampleWriteSupport(
        new TensorflowExampleSchemaConverter(conf).convert(schema), schema);
  }

  public static class Builder extends ParquetWriter.Builder<Example, Builder> {
    private Schema schema;

    protected Builder(OutputFile file) {
      super(file);
    }

    @Override
    protected Builder self() {
      return this;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    @Override
    protected WriteSupport<Example> getWriteSupport(Configuration conf) {
      return TensorflowExampleParquetWriter.writeSupport(conf, schema);
    }
  }
}
