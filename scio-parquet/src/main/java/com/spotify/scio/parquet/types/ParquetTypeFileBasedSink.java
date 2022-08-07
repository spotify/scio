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

package com.spotify.scio.parquet.types;

import com.spotify.scio.parquet.BeamOutputFile;
import com.spotify.scio.parquet.WriterUtils;
import magnolify.parquet.ParquetType;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.channels.WritableByteChannel;

public class ParquetTypeFileBasedSink<T> extends FileBasedSink<T, Void, T> {

  private final ParquetType<T> type;
  private final SerializableConfiguration conf;
  private final CompressionCodecName compression;

  public ParquetTypeFileBasedSink(
      ValueProvider<ResourceId> baseOutputFileName,
      FileBasedSink.DynamicDestinations<T, Void, T> dynamicDestinations,
      ParquetType<T> type,
      Configuration conf,
      CompressionCodecName compression) {
    super(baseOutputFileName, dynamicDestinations);
    this.type = type;
    this.conf = new SerializableConfiguration(conf);
    this.compression = compression;
  }

  @Override
  public FileBasedSink.WriteOperation<Void, T> createWriteOperation() {
    return new ParquetTypeWriteOperation<>(this, type, conf, compression);
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  static class ParquetTypeWriteOperation<T> extends WriteOperation<Void, T> {
    private final ParquetType<T> type;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;

    public ParquetTypeWriteOperation(
        FileBasedSink<T, Void, T> sink,
        ParquetType<T> type,
        SerializableConfiguration conf,
        CompressionCodecName compression) {
      super(sink);
      this.type = type;
      this.conf = conf;
      this.compression = compression;
    }

    @Override
    public Writer<Void, T> createWriter() throws Exception {
      return new ParquetTypeWriter<>(this, type, conf, compression);
    }
  }

  // =======================================================================
  // Writer
  // =======================================================================

  static class ParquetTypeWriter<T> extends FileBasedSink.Writer<Void, T> {

    private final ParquetType<T> type;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;
    private ParquetWriter<T> writer;

    public ParquetTypeWriter(
        WriteOperation<Void, T> writeOperation,
        ParquetType<T> type,
        SerializableConfiguration conf,
        CompressionCodecName compression) {
      super(writeOperation, MimeTypes.BINARY);
      this.type = type;
      this.conf = conf;
      this.compression = compression;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      BeamOutputFile outputFile = BeamOutputFile.of(channel);
      writer = WriterUtils.build(type.writeBuilder(outputFile), conf.get(), compression);
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
