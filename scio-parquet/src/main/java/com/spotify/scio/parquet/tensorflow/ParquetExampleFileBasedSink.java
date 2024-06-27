/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.parquet.tensorflow;

import com.spotify.parquet.tensorflow.TensorflowExampleParquetWriter;
import com.spotify.scio.parquet.BeamOutputFile;
import com.spotify.scio.parquet.WriterUtils;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class ParquetExampleFileBasedSink extends FileBasedSink<Example, Void, Example> {

  private final Schema schema;
  private final SerializableConfiguration conf;
  private final CompressionCodecName compression;
  private final Map<String, String> extraMetadata;

  public ParquetExampleFileBasedSink(
      ValueProvider<ResourceId> baseOutputFileName,
      FileBasedSink.DynamicDestinations<Example, Void, Example> dynamicDestinations,
      Schema schema,
      Configuration conf,
      CompressionCodecName compression,
      Map<String, String> extraMetadata) {
    super(baseOutputFileName, dynamicDestinations);
    this.schema = schema;
    this.conf = new SerializableConfiguration(conf);
    this.compression = compression;
    this.extraMetadata = extraMetadata;
  }

  @Override
  public FileBasedSink.WriteOperation<Void, Example> createWriteOperation() {
    return new ParquetExampleWriteOperation(this, schema, conf, compression, extraMetadata);
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  static class ParquetExampleWriteOperation extends FileBasedSink.WriteOperation<Void, Example> {
    private final Schema schema;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;
    private final Map<String, String> extraMetadata;

    ParquetExampleWriteOperation(
        FileBasedSink<Example, Void, Example> sink,
        Schema schema,
        SerializableConfiguration conf,
        CompressionCodecName compression,
        Map<String, String> extraMetadata) {
      super(sink);
      this.schema = schema;
      this.conf = conf;
      this.compression = compression;
      this.extraMetadata = extraMetadata;
    }

    @Override
    public Writer<Void, Example> createWriter() throws Exception {
      return new ParquetExampleWriter(this, schema, conf, compression, extraMetadata);
    }
  }

  // =======================================================================
  // Writer
  // =======================================================================

  static class ParquetExampleWriter extends FileBasedSink.Writer<Void, Example> {

    private final Schema schema;
    private final SerializableConfiguration conf;
    private final CompressionCodecName compression;
    private final Map<String, String> extraMetadata;
    private ParquetWriter<Example> writer;

    public ParquetExampleWriter(
        FileBasedSink.WriteOperation<Void, Example> writeOperation,
        Schema schema,
        SerializableConfiguration conf,
        CompressionCodecName compression,
        Map<String, String> extraMetadata) {
      super(writeOperation, MimeTypes.BINARY);
      this.schema = schema;
      this.conf = conf;
      this.compression = compression;
      this.extraMetadata = extraMetadata;
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      BeamOutputFile outputFile = BeamOutputFile.of(channel);
      TensorflowExampleParquetWriter.Builder builder =
          TensorflowExampleParquetWriter.builder(outputFile).withSchema(schema);
      writer = WriterUtils.build(builder, conf.get(), compression, extraMetadata);
    }

    @Override
    public void write(Example value) throws Exception {
      writer.write(value);
    }

    @Override
    protected void finishWrite() throws Exception {
      writer.close();
    }
  }
}
