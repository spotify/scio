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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.AvroFileOperations.SerializableSchemaSupplier;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/** API for reading and writing Parquet sorted-bucket files as Avro. */
public class ParquetAvroSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".parquet";

  // make sure parquet is part of the classpath
  static {
    try {
      Class.forName("org.apache.parquet.schema.Types");
    } catch (ClassNotFoundException e) {
      throw new MissingImplementationException("parquet", e);
    }
  }

  /** Returns a new {@link Read} for Avro generic records. */
  public static Read<GenericRecord> read(TupleTag<GenericRecord> tupleTag, Schema schema) {
    return new AutoValue_ParquetAvroSortedBucketIO_Read.Builder<GenericRecord>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setSchema(schema)
        .setConfiguration(new Configuration())
        .build();
  }

  /** Returns a new {@link Read} for Avro specific records. */
  public static <T extends SpecificRecord> Read<T> read(
      TupleTag<T> tupleTag, Class<T> recordClass) {
    return new AutoValue_ParquetAvroSortedBucketIO_Read.Builder<T>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setRecordClass(recordClass)
        .setConfiguration(new Configuration())
        .build();
  }

  /** Returns a new {@link Write} for Avro generic records. */
  public static <K1> Write<K1, Void, GenericRecord> write(
      Class<K1> keyClassPrimary, String keyFieldPrimary, Schema schema) {
    return ParquetAvroSortedBucketIO.<K1, Void, GenericRecord>newBuilder(
            keyClassPrimary, keyFieldPrimary, null, null)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link Write} for Avro generic records. */
  public static <K1, K2> Write<K1, K2, GenericRecord> write(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Schema schema) {
    return ParquetAvroSortedBucketIO.<K1, K2, GenericRecord>newBuilder(
            keyClassPrimary, keyFieldPrimary, keyClassSecondary, keyFieldSecondary)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link Write} for Avro specific records. */
  public static <K1, T extends SpecificRecord> Write<K1, Void, T> write(
      Class<K1> keyClassPrimary, String keyFieldPrimary, Class<T> recordClass) {
    return ParquetAvroSortedBucketIO.<K1, Void, T>newBuilder(
            keyClassPrimary, keyFieldPrimary, null, null)
        .setRecordClass(recordClass)
        .build();
  }

  /** Returns a new {@link Write} for Avro specific records. */
  public static <K1, K2, T extends SpecificRecord> Write<K1, K2, T> write(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Class<T> recordClass) {
    return ParquetAvroSortedBucketIO.<K1, K2, T>newBuilder(
            keyClassPrimary, keyFieldPrimary, keyClassSecondary, keyFieldSecondary)
        .setRecordClass(recordClass)
        .build();
  }

  private static <K1, K2, T extends IndexedRecord> Write.Builder<K1, K2, T> newBuilder(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary) {
    return new AutoValue_ParquetAvroSortedBucketIO_Write.Builder<K1, K2, T>()
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClassPrimary(keyClassPrimary)
        .setKeyClassSecondary(keyClassSecondary)
        .setKeyFieldPrimary(keyFieldPrimary)
        .setKeyFieldSecondary(keyFieldSecondary)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(ParquetAvroFileOperations.DEFAULT_COMPRESSION)
        .setConfiguration(new Configuration());
  }

  /** Returns a new {@link TransformOutput} for Avro generic records. */
  public static <K1> TransformOutput<K1, Void, GenericRecord> transformOutput(
      Class<K1> keyClassPrimary, String keyFieldPrimary, Schema schema) {
    return ParquetAvroSortedBucketIO.transformOutput(
        keyClassPrimary, keyFieldPrimary, null, null, schema);
  }

  /** Returns a new {@link TransformOutput} for Avro generic records. */
  public static <K1, K2> TransformOutput<K1, K2, GenericRecord> transformOutput(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Schema schema) {
    return new AutoValue_ParquetAvroSortedBucketIO_TransformOutput.Builder<K1, K2, GenericRecord>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCompression(ParquetAvroFileOperations.DEFAULT_COMPRESSION)
        .setConfiguration(new Configuration())
        .setKeyClassPrimary(keyClassPrimary)
        .setKeyClassSecondary(keyClassSecondary)
        .setKeyFieldPrimary(keyFieldPrimary)
        .setKeyFieldSecondary(keyFieldSecondary)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link TransformOutput} for Avro specific records. */
  public static <K1, T extends SpecificRecord> TransformOutput<K1, Void, T> transformOutput(
      Class<K1> keyClassPrimary, String keyFieldPrimary, Class<T> recordClass) {
    return ParquetAvroSortedBucketIO.transformOutput(
        keyClassPrimary, keyFieldPrimary, null, null, recordClass);
  }

  /** Returns a new {@link TransformOutput} for Avro specific records. */
  public static <K1, K2, T extends SpecificRecord> TransformOutput<K1, K2, T> transformOutput(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      Class<T> recordClass) {
    return new AutoValue_ParquetAvroSortedBucketIO_TransformOutput.Builder<K1, K2, T>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCompression(ParquetAvroFileOperations.DEFAULT_COMPRESSION)
        .setConfiguration(new Configuration())
        .setKeyClassPrimary(keyClassPrimary)
        .setKeyClassSecondary(keyClassSecondary)
        .setKeyFieldPrimary(keyFieldPrimary)
        .setKeyFieldSecondary(keyFieldSecondary)
        .setRecordClass(recordClass)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  /** Reads from Avro sorted-bucket files, to be used with {@link SortedBucketIO.CoGbk}. */
  @AutoValue
  public abstract static class Read<T extends IndexedRecord> extends SortedBucketIO.Read<T> {
    @Nullable
    abstract ImmutableList<String> getInputDirectories();

    abstract String getFilenameSuffix();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract FilterPredicate getFilterPredicate();

    @Nullable
    abstract Predicate<T> getPredicate();

    abstract Configuration getConfiguration();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T extends IndexedRecord> {
      abstract Builder<T> setTupleTag(TupleTag<T> tupleTag);

      abstract Builder<T> setInputDirectories(List<String> inputDirectories);

      abstract Builder<T> setInputDirectories(String... inputDirectory);

      abstract Builder<T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setFilterPredicate(FilterPredicate predicate);

      abstract Builder<T> setPredicate(Predicate<T> predicate);

      abstract Builder<T> setConfiguration(Configuration configuration);

      abstract Read<T> build();
    }

    /** Reads from the given input directory. */
    public Read<T> from(String... inputDirectories) {
      return from(Arrays.asList(inputDirectories));
    }

    /** Reads from the given input directories. */
    public Read<T> from(List<String> inputDirectories) {
      return toBuilder().setInputDirectories(inputDirectories).build();
    }

    /** Specifies the input filename suffix. */
    public Read<T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the Parquet filter predicate. */
    public Read<T> withFilterPredicate(FilterPredicate predicate) {
      return toBuilder().setFilterPredicate(predicate).build();
    }

    /** Specifies the filter predicate. */
    public Read<T> withPredicate(Predicate<T> predicate) {
      return toBuilder().setPredicate(predicate).build();
    }

    /** Specifies the Hadoop {@link Configuration}. */
    public Read<T> withConfiguration(Configuration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    @Override
    protected BucketedInput<T> toBucketedInput(final SortedBucketSource.Keying keying) {
      final Schema schema =
          getRecordClass() == null
              ? getSchema()
              : new ReflectData(getRecordClass().getClassLoader()).getSchema(getRecordClass());
      final ParquetAvroFileOperations<T> fileOperations =
          ParquetAvroFileOperations.of(schema, getFilterPredicate(), getConfiguration());
      return BucketedInput.of(
          keying,
          getTupleTag(),
          getInputDirectories(),
          getFilenameSuffix(),
          fileOperations,
          getPredicate());
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  /** Writes to Avro sorted-bucket files using {@link SortedBucketSink}. */
  @AutoValue
  public abstract static class Write<K1, K2, T extends IndexedRecord>
      extends SortedBucketIO.Write<K1, K2, T> {

    @Nullable
    abstract String getKeyFieldPrimary();

    @Nullable
    abstract String getKeyFieldSecondary();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CompressionCodecName getCompression();

    abstract Configuration getConfiguration();

    abstract Builder<K1, K2, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K1, K2, T extends IndexedRecord> {
      // Common
      abstract Builder<K1, K2, T> setNumBuckets(int numBuckets);

      abstract Builder<K1, K2, T> setNumShards(int numShards);

      abstract Builder<K1, K2, T> setKeyClassPrimary(Class<K1> keyClassPrimary);

      abstract Builder<K1, K2, T> setKeyClassSecondary(Class<K2> keyClassSecondary);

      abstract Builder<K1, K2, T> setHashType(HashType hashType);

      abstract Builder<K1, K2, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K1, K2, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K1, K2, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K1, K2, T> setSorterMemoryMb(int sorterMemoryMb);

      // Avro specific
      abstract Builder<K1, K2, T> setKeyFieldPrimary(String keyFieldPrimary);

      abstract Builder<K1, K2, T> setKeyFieldSecondary(String keyFieldSecondary);

      abstract Builder<K1, K2, T> setSchema(Schema schema);

      abstract Builder<K1, K2, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K1, K2, T> setCompression(CompressionCodecName compression);

      abstract Builder<K1, K2, T> setConfiguration(Configuration conf);

      abstract Builder<K1, K2, T> setKeyCacheSize(int cacheSize);

      abstract Builder<K1, K2, T> setFilenamePrefix(String filenamePrefix);

      abstract Write<K1, K2, T> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K1, K2, T> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K1, K2, T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K1, K2, T> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K1, K2, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public Write<K1, K2, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    @Override
    FileOperations<T> getFileOperations() {
      return getRecordClass() == null
          ? ParquetAvroFileOperations.of(getSchema(), getCompression(), getConfiguration())
          : ParquetAvroFileOperations.of(getRecordClass(), getCompression(), getConfiguration());
    }

    @Override
    BucketMetadata<K1, K2, T> getBucketMetadata() {
      final Schema schema =
          getRecordClass() == null
              ? getSchema()
              : new ReflectData(getRecordClass().getClassLoader()).getSchema(getRecordClass());
      try {
        return new ParquetBucketMetadata<>(
            getNumBuckets(),
            getNumShards(),
            getKeyClassPrimary(),
            getKeyFieldPrimary(),
            getKeyClassSecondary(),
            getKeyFieldSecondary(),
            getHashType(),
            getFilenamePrefix(),
            schema);
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }
    }

    /** Specifies the output filename suffix. */
    public Write<K1, K2, T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix (i.e. "bucket" or "part"). */
    public Write<K1, K2, T> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K1, K2, T> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the size of an optional key-to-hash cache in the ExtractKeys transform. */
    public Write<K1, K2, T> withKeyCacheOfSize(int keyCacheSize) {
      return toBuilder().setKeyCacheSize(keyCacheSize).build();
    }

    /** Specifies the output file {@link CompressionCodecName}. */
    public Write<K1, K2, T> withCompression(CompressionCodecName compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** Specifies the Hadoop {@link Configuration}. */
    public Write<K1, K2, T> withConfiguration(Configuration conf) {
      return toBuilder().setConfiguration(conf).build();
    }
  }

  /** Writes to Avro sorted-bucket files using {@link SortedBucketTransform}. */
  @AutoValue
  public abstract static class TransformOutput<K1, K2, T extends IndexedRecord>
      extends SortedBucketIO.TransformOutput<K1, K2, T> {
    @Nullable
    abstract String getKeyFieldPrimary();

    @Nullable
    abstract String getKeyFieldSecondary();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CompressionCodecName getCompression();

    abstract Configuration getConfiguration();

    abstract Builder<K1, K2, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K1, K2, T extends IndexedRecord> {
      abstract Builder<K1, K2, T> setKeyClassPrimary(Class<K1> keyClassPrimary);

      abstract Builder<K1, K2, T> setKeyClassSecondary(Class<K2> keyClassSecondary);

      abstract Builder<K1, K2, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K1, K2, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K1, K2, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K1, K2, T> setFilenamePrefix(String filenamePrefix);

      // Avro specific
      abstract Builder<K1, K2, T> setKeyFieldPrimary(String keyFieldPrimary);

      abstract Builder<K1, K2, T> setKeyFieldSecondary(String keyFieldSecondary);

      abstract Builder<K1, K2, T> setSchema(Schema schema);

      abstract Builder<K1, K2, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K1, K2, T> setCompression(CompressionCodecName compression);

      abstract Builder<K1, K2, T> setConfiguration(Configuration conf);

      abstract TransformOutput<K1, K2, T> build();
    }

    /** Writes to the given output directory. */
    public TransformOutput<K1, K2, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public TransformOutput<K1, K2, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public TransformOutput<K1, K2, T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix. */
    public TransformOutput<K1, K2, T> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the output file {@link CompressionCodecName}. */
    public TransformOutput<K1, K2, T> withCompression(CompressionCodecName compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** Specifies the output file {@link Configuration}. */
    public TransformOutput<K1, K2, T> withConfiguration(Configuration conf) {
      return toBuilder().setConfiguration(conf).build();
    }

    @SuppressWarnings("unchecked")
    @Override
    FileOperations<T> getFileOperations() {
      final Schema schema =
          getRecordClass() == null
              ? getSchema()
              : new ReflectData(getRecordClass().getClassLoader()).getSchema(getRecordClass());
      return ParquetAvroFileOperations.of(schema, getCompression(), getConfiguration());
    }

    @Override
    NewBucketMetadataFn<K1, K2, T> getNewBucketMetadataFn() {
      final String keyFieldPrimary = getKeyFieldPrimary();
      final String keyFieldSeconary = getKeyFieldSecondary();
      final Class<K1> keyClassPrimary = getKeyClassPrimary();
      final Class<K2> keyClassSecondary = getKeyClassSecondary();
      final String filenamePrefix = getFilenamePrefix();

      final Schema schema =
          getRecordClass() == null
              ? getSchema()
              : new ReflectData(getRecordClass().getClassLoader()).getSchema(getRecordClass());
      final SerializableSchemaSupplier schemaSupplier = new SerializableSchemaSupplier(schema);
      return (numBuckets, numShards, hashType) -> {
        try {
          return new ParquetBucketMetadata<K1, K2, T>(
              numBuckets,
              numShards,
              keyClassPrimary,
              keyFieldPrimary,
              keyClassSecondary,
              keyFieldSeconary,
              hashType,
              filenamePrefix,
              schemaSupplier.get());
        } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
          throw new IllegalStateException(e);
        }
      };
    }
  }
}
