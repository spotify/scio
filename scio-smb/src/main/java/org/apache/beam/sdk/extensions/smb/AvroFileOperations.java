/*
 * Copyright 2019 Spotify AB.
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
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.PatchedSerializableAvroCodecFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;

/** {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for Avro files. */
public class AvroFileOperations<ValueT> extends FileOperations<ValueT> {

  private final AvroDatumFactory<ValueT> datumFactory;
  private final SerializableSchemaSupplier schemaSupplier;
  private PatchedSerializableAvroCodecFactory codec;
  private Map<String, Object> metadata;

  static CodecFactory defaultCodec() {
    return CodecFactory.deflateCodec(6);
  }

  private AvroFileOperations(AvroDatumFactory<ValueT> datumFactory, Schema schema) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY); // Avro has its own compression via codec
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
    this.datumFactory = datumFactory;
    this.codec = new PatchedSerializableAvroCodecFactory(defaultCodec());
  }

  public static <V extends IndexedRecord> AvroFileOperations<V> of(
      AvroDatumFactory<V> datumFactory, Schema schema) {
    return new AvroFileOperations<>(datumFactory, schema);
  }

  public AvroFileOperations<ValueT> withCodec(CodecFactory codec) {
    this.codec = new PatchedSerializableAvroCodecFactory(codec);
    return this;
  }

  public AvroFileOperations<ValueT> withMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
    return this;
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("codecFactory", codec.getCodec().getClass()));
    builder.add(DisplayData.item("schema", schemaSupplier.schema.getFullName()));
  }

  @Override
  protected Reader<ValueT> createReader() {
    return new AvroReader<>(datumFactory, schemaSupplier);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FileIO.Sink<ValueT> createSink() {
    final AvroIO.Sink<ValueT> sink =
        ((AvroIO.Sink<ValueT>) AvroIO.sink(getSchema()))
            .withDatumWriterFactory(datumFactory)
            .withCodec(codec.getCodec());

    if (metadata != null) {
      return sink.withMetadata(metadata);
    } else {
      return sink;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Coder<ValueT> getCoder() {
    return AvroCoder.of(datumFactory, getSchema());
  }

  Schema getSchema() {
    return schemaSupplier.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSchema(), codec.getCodec(), metadata, datumFactory);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AvroFileOperations)) {
      return false;
    }
    final AvroFileOperations<ValueT> that = (AvroFileOperations<ValueT>) obj;
    return that.getSchema().equals(this.getSchema())
        && that.codec.getCodec().toString().equals(this.codec.getCodec().toString())
        && ((that.metadata == null && this.metadata == null)
            || (this.metadata.equals(that.metadata)))
        && that.datumFactory.equals(this.datumFactory);
  }

  private static class SerializableSchemaString implements Serializable {
    private final String schema;

    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(new Schema.Parser().parse(schema));
    }
  }

  static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {
    private transient Schema schema;

    SerializableSchemaSupplier(Schema schema) {
      this.schema = schema;
    }

    private Object writeReplace() {
      return new SerializableSchemaString(schema.toString());
    }

    @Override
    public Schema get() {
      return schema;
    }
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class AvroReader<ValueT> extends FileOperations.Reader<ValueT> {
    private AvroDatumFactory<ValueT> datumFactory;
    private SerializableSchemaSupplier schemaSupplier;
    private transient DataFileStream<ValueT> reader;

    AvroReader(AvroDatumFactory<ValueT> datumFactory, SerializableSchemaSupplier schemaSupplier) {
      this.datumFactory = datumFactory;
      this.schemaSupplier = schemaSupplier;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      final Schema schema = schemaSupplier.get();

      DatumReader<ValueT> datumReader = datumFactory.apply(schema, schema);
      reader = new DataFileStream<>(Channels.newInputStream(channel), datumReader);
    }

    @Override
    public ValueT readNext() {
      return reader.next();
    }

    @Override
    public boolean hasNextElement() {
      return reader.hasNext();
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }
}
