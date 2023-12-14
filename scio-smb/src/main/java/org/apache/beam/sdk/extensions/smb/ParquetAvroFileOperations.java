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

package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.smb.AvroFileOperations.SerializableSchemaSupplier;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroDataSupplier;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.avro.GenericDataSupplier;
import org.apache.parquet.avro.SpecificDataSupplier;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for Parquet files with
 * Avro records.
 */
public class ParquetAvroFileOperations<ValueT> extends FileOperations<ValueT> {
  static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.ZSTD;

  private final Class<ValueT> recordClass;
  private final SerializableSchemaSupplier schemaSupplier;

  private SerializableSchemaSupplier projectionSupplier;
  private CompressionCodecName compression;
  private SerializableConfiguration conf;
  private FilterPredicate predicate;

  private ParquetAvroFileOperations(Schema schema, Class<ValueT> recordClass) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
    this.recordClass = recordClass;
    this.compression = DEFAULT_COMPRESSION;
    this.conf = new SerializableConfiguration(new Configuration());
  }

  public static ParquetAvroFileOperations<GenericRecord> of(Schema schema) {
    return new ParquetAvroFileOperations(schema, null);
  }

  public static <V extends IndexedRecord> ParquetAvroFileOperations<V> of(Class<V> recordClass) {
    return new ParquetAvroFileOperations(
        new ReflectData(recordClass.getClassLoader()).getSchema(recordClass), recordClass);
  }

  public ParquetAvroFileOperations<ValueT> withConfiguration(Configuration configuration) {
    this.conf = new SerializableConfiguration(configuration);
    return this;
  }

  public ParquetAvroFileOperations<ValueT> withCompression(CompressionCodecName compression) {
    this.compression = compression;
    return this;
  }

  public ParquetAvroFileOperations<ValueT> withFilterPredicate(FilterPredicate filterPredicate) {
    this.predicate = filterPredicate;
    return this;
  }

  public ParquetAvroFileOperations<ValueT> withProjection(Schema projection) {
    this.projectionSupplier =
        (projection != null) ? new SerializableSchemaSupplier(projection) : null;
    return this;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("compressionCodecName", compression.name()));
    builder.add(DisplayData.item("schema", schemaSupplier.get().getFullName()));
  }

  @Override
  protected Reader<ValueT> createReader() {
    return new ParquetAvroReader<>(schemaSupplier, projectionSupplier, conf, predicate);
  }

  @Override
  protected FileIO.Sink<ValueT> createSink() {
    return new ParquetAvroSink<>(schemaSupplier, compression, conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Coder<ValueT> getCoder() {
    return recordClass == null
        ? (AvroCoder<ValueT>) AvroCoder.of(getSchema())
        : AvroCoder.reflect(recordClass);
  }

  Schema getSchema() {
    return schemaSupplier.get();
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class ParquetAvroReader<ValueT> extends FileOperations.Reader<ValueT> {
    private final SerializableSchemaSupplier readSchemaSupplier;

    private final SerializableSchemaSupplier projectionSchemaSupplier;
    private final SerializableConfiguration conf;
    private final FilterPredicate predicate;
    private transient ParquetReader<ValueT> reader;
    private transient ValueT current;

    private ParquetAvroReader(
        SerializableSchemaSupplier readSchemaSupplier,
        SerializableSchemaSupplier projectionSchemaSupplier,
        SerializableConfiguration conf,
        FilterPredicate predicate) {
      this.readSchemaSupplier = readSchemaSupplier;
      this.projectionSchemaSupplier = projectionSchemaSupplier;
      this.conf = conf;
      this.predicate = predicate;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      final Schema readSchema = readSchemaSupplier.get();
      final Configuration configuration = conf.get();
      AvroReadSupport.setAvroReadSchema(configuration, readSchema);

      if (projectionSchemaSupplier != null) {
        AvroReadSupport.setRequestedProjection(configuration, projectionSchemaSupplier.get());
      }

      ParquetReader.Builder<ValueT> builder =
          AvroParquetReader.<ValueT>builder(new ParquetInputFile(channel)).withConf(configuration);
      if (predicate != null) {
        builder = builder.withFilter(FilterCompat.get(predicate));
      }
      reader = builder.build();
      current = reader.read();
    }

    @Override
    public ValueT readNext() throws IOException, NoSuchElementException {
      ValueT r = current;
      current = reader.read();
      return r;
    }

    @Override
    public boolean hasNextElement() throws IOException {
      return current != null;
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }

  ////////////////////////////////////////
  // Sink
  ////////////////////////////////////////

  private static class ParquetAvroSink<ValueT> implements FileIO.Sink<ValueT> {
    private final SerializableSchemaSupplier schemaSupplier;
    private final CompressionCodecName compression;
    private final SerializableConfiguration conf;
    private transient ParquetWriter<ValueT> writer;

    private ParquetAvroSink(
        SerializableSchemaSupplier schemaSupplier,
        CompressionCodecName compression,
        SerializableConfiguration conf) {
      this.schemaSupplier = schemaSupplier;
      this.compression = compression;
      this.conf = conf;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
      final Configuration configuration = conf.get();

      AvroParquetWriter.Builder<ValueT> builder =
          AvroParquetWriter.<ValueT>builder(new ParquetOutputFile(channel))
              .withSchema(schemaSupplier.get());

      // Workaround for PARQUET-2265
      if (configuration.getClass(AvroWriteSupport.AVRO_DATA_SUPPLIER, null) != null) {
        Class<? extends AvroDataSupplier> dataModelSupplier =
            configuration.getClass(
                AvroWriteSupport.AVRO_DATA_SUPPLIER,
                SpecificDataSupplier.class,
                AvroDataSupplier.class);
        builder =
            builder.withDataModel(
                ReflectionUtils.newInstance(dataModelSupplier, configuration).get());
      }

      writer = ParquetUtils.buildWriter(builder, configuration, compression);
    }

    @Override
    public void write(ValueT element) throws IOException {
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      writer.close();
    }
  }
}
