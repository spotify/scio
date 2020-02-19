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

import com.spotify.scio.parquet.BeamOutputFile;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.channels.WritableByteChannel;

public class ParquetAvroSink<T> extends FileBasedSink<T, Void, T> {

  private final String schemaString;
  private final SerializableConfiguration conf;
  private final CompressionCodecName compression;

  public ParquetAvroSink(ValueProvider<ResourceId> baseOutputFileName,
                         FileBasedSink.DynamicDestinations<T, Void, T> dynamicDestinations,
                         Schema schema,
                         Configuration conf,
                         CompressionCodecName compression) {
    super(baseOutputFileName, dynamicDestinations);
    this.schemaString = schema.toString();
    this.conf = new SerializableConfiguration(conf);
    this.compression = compression;
  }

  @Override
  public FileBasedSink.WriteOperation<Void, T> createWriteOperation() {
    return new ParquetAvroWriteOperation<T>(this, schemaString, conf, compression);
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  static class ParquetAvroWriteOperation<T> extends WriteOperation<Void, T> {

    private final String schemaString;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;

    public ParquetAvroWriteOperation(FileBasedSink<T, Void, T> sink,
                                     String schemaString,
                                     SerializableConfiguration conf,
                                     CompressionCodecName compression) {
      super(sink);
      this.schemaString = schemaString;
      this.conf = conf;
      this.compression = compression;
    }

    @Override
    public Writer<Void, T> createWriter() throws Exception {
      return new ParquetAvroWriter<>(this, new Schema.Parser().parse(schemaString), conf, compression);
    }
  }

  // =======================================================================
  // Writer
  // =======================================================================

  static class ParquetAvroWriter<T> extends FileBasedSink.Writer<Void, T> {

    private final Schema schema;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;
    private ParquetWriter<T> writer;

    public ParquetAvroWriter(WriteOperation<Void, T> writeOperation,
                             Schema schema,
                             SerializableConfiguration conf,
                             CompressionCodecName compression) {
      super(writeOperation, MimeTypes.BINARY);
      this.schema = schema;
      this.conf = conf;
      this.compression = compression;
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      BeamOutputFile outputFile = BeamOutputFile.of(channel);
      writer = org.apache.parquet.avro.AvroParquetWriter.<T>builder(outputFile)
              .withSchema(schema)
              .withConf(conf.get())
              .withCompressionCodec(compression)
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
