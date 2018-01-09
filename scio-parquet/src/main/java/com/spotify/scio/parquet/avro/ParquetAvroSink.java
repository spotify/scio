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

package com.spotify.scio.parquet.avro;

import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.HadoopFileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import javax.annotation.Nullable;

public class ParquetAvroSink<T> extends HadoopFileBasedSink<T, Void, T> {

  private final String schemaString;
  private final SerializableConfiguration conf;

  public ParquetAvroSink(ValueProvider<ResourceId> baseOutputFileName,
                         DynamicDestinations<T, Void, T> dynamicDestinations,
                         Schema schema,
                         Configuration conf) {
    super(baseOutputFileName, dynamicDestinations);
    schemaString = schema.toString();
    this.conf = new SerializableConfiguration(conf);
  }

  @Override
  public WriteOperation<Void, T> createWriteOperation() {
    return new ParquetAvroWriteOperation<T>(this, schemaString, conf);
  }

  @Override
  public OutputFileHints getOutputFileHints() {
    return new OutputFileHints() {
      @Nullable
      @Override
      public String getMimeType() {
        return MimeTypes.BINARY;
      }

      @Nullable
      @Override
      public String getSuggestedFilenameSuffix() {
        return ".parquet";
      }
    };
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  static class ParquetAvroWriteOperation<T> extends WriteOperation<Void, T> {

    private final String schemaString;
    private final SerializableConfiguration conf;

    public ParquetAvroWriteOperation(HadoopFileBasedSink<T, Void, T> sink,
                                     String schemaString,
                                     SerializableConfiguration conf) {
      super(sink);
      this.schemaString = schemaString;
      this.conf = conf;
    }

    @Override
    public Writer<Void, T> createWriter() throws Exception {
      return new ParquetAvroWriter<>(this, new Schema.Parser().parse(schemaString), conf);
    }
  }

  // =======================================================================
  // Writer
  // =======================================================================

  static class ParquetAvroWriter<T> extends Writer<Void, T> {

    private final Schema schema;
    private final SerializableConfiguration conf;
    private ParquetWriter<T> writer;

    public ParquetAvroWriter(WriteOperation<Void, T> writeOperation,
                             Schema schema,
                             SerializableConfiguration conf) {
      super(writeOperation);
      this.schema = schema;
      this.conf = conf;
    }

    @Override
    protected void prepareWrite(Path path) throws Exception {
      writer = org.apache.parquet.avro.AvroParquetWriter.<T>builder(path)
          .withSchema(schema)
          .withConf(conf.get())
          .build();
    }

    @Override
    public void write(T value) throws Exception {
      writer.write(value);
    }

    @Override
    protected void finishWrite() throws Exception {
      writer.close();
    }
  }

}
