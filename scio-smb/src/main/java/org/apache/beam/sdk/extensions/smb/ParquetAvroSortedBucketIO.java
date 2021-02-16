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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** API for reading and writing Parquet sorted-bucket files as Avro. */
public class ParquetAvroSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".parquet";
  /** Returns a new {@link Read} for Avro generic records. */
  public static Read<GenericRecord> read(TupleTag<GenericRecord> tupleTag, Schema schema) {
    return new AutoValue_ParquetAvroSortedBucketIO_Read.Builder<>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link Write} for Avro generic records. */
  public static <K> Write<K, GenericRecord> write(
      Class<K> keyClass, String keyField, Schema schema) {
    return ParquetAvroSortedBucketIO.newBuilder(keyClass, keyField).setSchema(schema).build();
  }

  private static <K, T extends GenericRecord> Write.Builder<K, T> newBuilder(
      Class<K> keyClass, String keyField) {
    return new AutoValue_ParquetAvroSortedBucketIO_Write.Builder<K, T>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(ParquetAvroFileOperations.DEFAULT_COMPRESSION)
        .setConfiguration(new Configuration());
  }

  /** Returns a new {@link TransformOutput} for Avro generic records. */
  public static <K> TransformOutput<K, org.apache.avro.generic.GenericRecord> transformOutput(
      Class<K> keyClass, String keyField, Schema schema) {
    return new AutoValue_ParquetAvroSortedBucketIO_TransformOutput.Builder<K, GenericRecord>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCompression(ParquetAvroFileOperations.DEFAULT_COMPRESSION)
        .setConfiguration(new Configuration())
        .setKeyField(keyField)
        .setKeyClass(keyClass)
        .setSchema(schema)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  /** Reads from Avro sorted-bucket files, to be used with {@link SortedBucketIO.CoGbk}. */
  @AutoValue
  public abstract static class Read<T extends GenericRecord> extends SortedBucketIO.Read<T> {
    @Nullable
    abstract ImmutableList<ResourceId> getInputDirectories();

    abstract String getFilenameSuffix();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract FilterPredicate getFilterPredicate();

    @Nullable
    abstract Predicate<T> getPredicate();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T extends GenericRecord> {
      abstract Builder<T> setTupleTag(TupleTag<T> tupleTag);

      abstract Builder<T> setInputDirectories(List<ResourceId> inputDirectories);

      abstract Builder<T> setInputDirectories(ResourceId... inputDirectory);

      abstract Builder<T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setFilterPredicate(FilterPredicate predicate);

      abstract Builder<T> setPredicate(Predicate<T> predicate);

      abstract Read<T> build();
    }

    /** Reads from the given input directory. */
    public Read<T> from(String... inputDirectories) {
      return from(Arrays.asList(inputDirectories));
    }

    /** Reads from the given input directories. */
    public Read<T> from(List<String> inputDirectories) {
      return toBuilder()
          .setInputDirectories(
              inputDirectories.stream()
                  .map(dir -> FileSystems.matchNewResource(dir, true))
                  .collect(Collectors.toList()))
          .build();
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

    @Override
    protected BucketedInput<?, T> toBucketedInput() {
      @SuppressWarnings("unchecked")
      final ParquetAvroFileOperations<T> fileOperations =
          ParquetAvroFileOperations.of(getSchema(), getFilterPredicate());
      return new BucketedInput<>(
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
  public abstract static class Write<K, T extends GenericRecord>
      extends SortedBucketIO.Write<K, T> {
    @Nullable
    abstract String getKeyField();

    @Nullable
    abstract Schema getSchema();

    abstract CompressionCodecName getCompression();

    abstract Configuration getConfiguration();

    abstract Builder<K, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, T extends GenericRecord> {
      // Common
      abstract Builder<K, T> setNumBuckets(int numBuckets);

      abstract Builder<K, T> setNumShards(int numShards);

      abstract Builder<K, T> setKeyClass(Class<K> keyClass);

      abstract Builder<K, T> setHashType(HashType hashType);

      abstract Builder<K, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K, T> setSorterMemoryMb(int sorterMemoryMb);

      // Avro specific
      abstract Builder<K, T> setKeyField(String keyField);

      abstract Builder<K, T> setSchema(Schema schema);

      abstract Builder<K, T> setCompression(CompressionCodecName compression);

      abstract Builder<K, T> setConfiguration(Configuration conf);

      abstract Builder<K, T> setKeyCacheSize(int cacheSize);

      abstract Builder<K, T> setFilenamePrefix(String filenamePrefix);

      abstract Write<K, T> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K, T> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K, T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K, T> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public Write<K, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    FileOperations<T> getFileOperations() {
      return ParquetAvroFileOperations.of(getSchema(), getCompression(), getConfiguration());
    }

    @SuppressWarnings("unchecked")
    @Override
    BucketMetadata<K, T> getBucketMetadata() {
      try {
        return new ParquetBucketMetadata<>(
            getNumBuckets(),
            getNumShards(),
            getKeyClass(),
            getHashType(),
            getKeyField(),
            getFilenamePrefix(),
            getSchema());
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }
    }

    /** Specifies the output filename suffix. */
    public Write<K, T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix (i.e. "bucket" or "part"). */
    public Write<K, T> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K, T> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the size of an optional key-to-hash cache in the ExtractKeys transform. */
    public Write<K, T> withKeyCacheOfSize(int keyCacheSize) {
      return toBuilder().setKeyCacheSize(keyCacheSize).build();
    }

    /** Specifies the output file {@link CompressionCodecName}. */
    public Write<K, T> withCompression(CompressionCodecName compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** Specifies the output file {@link Configuration}. */
    public Write<K, T> withConfiguration(Configuration conf) {
      return toBuilder().setConfiguration(conf).build();
    }
  }

  /** Writes to Avro sorted-bucket files using {@link SortedBucketTransform}. */
  @AutoValue
  public abstract static class TransformOutput<K, T extends GenericRecord>
      extends SortedBucketIO.TransformOutput<K, T> {
    @Nullable
    abstract String getKeyField();

    @Nullable
    abstract Schema getSchema();

    abstract CompressionCodecName getCompression();

    abstract Configuration getConfiguration();

    abstract Builder<K, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, T extends GenericRecord> {
      abstract Builder<K, T> setKeyClass(Class<K> keyClass);

      abstract Builder<K, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K, T> setFilenamePrefix(String filenamePrefix);

      // Avro specific
      abstract Builder<K, T> setKeyField(String keyField);

      abstract Builder<K, T> setSchema(Schema schema);

      abstract Builder<K, T> setCompression(CompressionCodecName compression);

      abstract Builder<K, T> setConfiguration(Configuration conf);

      abstract TransformOutput<K, T> build();
    }

    /** Writes to the given output directory. */
    public TransformOutput<K, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public TransformOutput<K, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public TransformOutput<K, T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix. */
    public TransformOutput<K, T> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the output file {@link CompressionCodecName}. */
    public TransformOutput<K, T> withCompression(CompressionCodecName compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** Specifies the output file {@link Configuration}. */
    public TransformOutput<K, T> withConfiguration(Configuration conf) {
      return toBuilder().setConfiguration(conf).build();
    }

    @SuppressWarnings("unchecked")
    @Override
    FileOperations<T> getFileOperations() {
      return ParquetAvroFileOperations.of(getSchema(), getCompression(), getConfiguration());
    }

    @Override
    NewBucketMetadataFn<K, T> getNewBucketMetadataFn() {
      final String keyField = getKeyField();
      final Class<K> keyClass = getKeyClass();
      final String filenamePrefix = getFilenamePrefix();

      final SerializableSchemaSupplier schemaSupplier = new SerializableSchemaSupplier(getSchema());

      return (numBuckets, numShards, hashType) -> {
        try {
          return new ParquetBucketMetadata<>(
              numBuckets,
              numShards,
              keyClass,
              hashType,
              keyField,
              filenamePrefix,
              schemaSupplier.get());
        } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
          throw new IllegalStateException(e);
        }
      };
    }
  }
}
