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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.AvroFileOperations.SerializableSchemaSupplier;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** API for reading and writing Avro sorted-bucket files. */
public class AvroSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".avro";
  /** Returns a new {@link Read} for Avro generic records. */
  public static Read<GenericRecord> read(TupleTag<GenericRecord> tupleTag, Schema schema) {
    return new AutoValue_AvroSortedBucketIO_Read.Builder<>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(AvroFileOperations.defaultCodec())
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link Read} for Avro specific records. */
  public static <T extends SpecificRecordBase> Read<T> read(
      TupleTag<T> tupleTag, Class<T> recordClass) {
    return new AutoValue_AvroSortedBucketIO_Read.Builder<T>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(AvroFileOperations.defaultCodec())
        .setRecordClass(recordClass)
        .build();
  }

  /** Returns a new {@link Write} for Avro generic records. */
  public static <K> Write<K, GenericRecord> write(
      Class<K> keyClass, String keyField, Schema schema) {
    return AvroSortedBucketIO.newBuilder(keyClass, keyField).setSchema(schema).build();
  }

  /** Returns a new {@link Write} for Avro specific records. */
  public static <K, T extends SpecificRecordBase> Write<K, T> write(
      Class<K> keyClass, String keyField, Class<T> recordClass) {
    return AvroSortedBucketIO.<K, T>newBuilder(keyClass, keyField)
        .setRecordClass(recordClass)
        .build();
  }

  private static <K, T extends GenericRecord> Write.Builder<K, T> newBuilder(
      Class<K> keyClass, String keyField) {
    return new AutoValue_AvroSortedBucketIO_Write.Builder<K, T>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(AvroFileOperations.defaultCodec());
  }

  /** Returns a new {@link TransformOutput} for Avro generic records. */
  public static <K> TransformOutput<K, org.apache.avro.generic.GenericRecord> transformOutput(
      Class<K> keyClass, String keyField, Schema schema) {
    return new AutoValue_AvroSortedBucketIO_TransformOutput.Builder<K, GenericRecord>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCodec(AvroFileOperations.defaultCodec())
        .setKeyField(keyField)
        .setKeyClass(keyClass)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link TransformOutput} for Avro specific records. */
  public static <K, T extends SpecificRecordBase> TransformOutput<K, T> transformOutput(
      Class<K> keyClass, String keyField, Class<T> recordClass) {
    return new AutoValue_AvroSortedBucketIO_TransformOutput.Builder<K, T>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCodec(AvroFileOperations.defaultCodec())
        .setKeyField(keyField)
        .setKeyClass(keyClass)
        .setRecordClass(recordClass)
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
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

    @Nullable
    abstract Map<String, Object> getMetadata();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T extends GenericRecord> {
      abstract Builder<T> setTupleTag(TupleTag<T> tupleTag);

      abstract Builder<T> setInputDirectories(List<ResourceId> inputDirectories);

      abstract Builder<T> setInputDirectories(ResourceId... inputDirectory);

      abstract Builder<T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setCodec(CodecFactory codec);

      abstract Builder<T> setMetadata(Map<String, Object> metadata);

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

    @Override
    protected BucketedInput<?, T> toBucketedInput() {
      @SuppressWarnings("unchecked")
      final AvroFileOperations<T> fileOperations =
          getRecordClass() == null
              ? AvroFileOperations.of(getSchema(), getCodec(), getMetadata())
              : (AvroFileOperations<T>)
                  AvroFileOperations.of(
                      (Class<SpecificRecordBase>) getRecordClass(), getCodec(), getMetadata());
      return new BucketedInput<>(
          getTupleTag(), getInputDirectories(), getFilenameSuffix(), fileOperations);
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

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

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

      abstract Builder<K, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K, T> setCodec(CodecFactory codec);

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

    /** Specifies the temporary directory for writing. */
    public Write<K, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    FileOperations<T> getFileOperations() {
      return getRecordClass() == null
          ? AvroFileOperations.of(getSchema(), getCodec())
          : (AvroFileOperations<T>)
              AvroFileOperations.of((Class<SpecificRecordBase>) getRecordClass(), getCodec());
    }

    @SuppressWarnings("unchecked")
    @Override
    BucketMetadata<K, T> getBucketMetadata() {
      try {
        return getRecordClass() == null
            ? new AvroBucketMetadata<>(
                getNumBuckets(),
                getNumShards(),
                getKeyClass(),
                getHashType(),
                getKeyField(),
                getFilenamePrefix(),
                getSchema())
            : (AvroBucketMetadata<K, T>)
                new AvroBucketMetadata<>(
                    getNumBuckets(),
                    getNumShards(),
                    getKeyClass(),
                    getHashType(),
                    getKeyField(),
                    getFilenamePrefix(),
                    (Class<SpecificRecordBase>) getRecordClass());
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

    /** Specifies the output file {@link CodecFactory}. */
    public Write<K, T> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(codec).build();
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

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

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

      abstract Builder<K, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K, T> setCodec(CodecFactory codec);

      abstract TransformOutput<K, T> build();
    }

    /** Writes to the given output directory. */
    public TransformOutput<K, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. */
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

    /** Specifies the output file {@link CodecFactory}. */
    public TransformOutput<K, T> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(codec).build();
    }

    @SuppressWarnings("unchecked")
    @Override
    FileOperations<T> getFileOperations() {
      return getRecordClass() == null
          ? AvroFileOperations.of(getSchema(), getCodec())
          : (AvroFileOperations<T>)
              AvroFileOperations.of((Class<SpecificRecordBase>) getRecordClass(), getCodec());
    }

    @Override
    NewBucketMetadataFn<K, T> getNewBucketMetadataFn() {
      final String keyField = getKeyField();
      final Class<K> keyClass = getKeyClass();
      final String filenamePrefix = getFilenamePrefix();
      final Class<T> recordClass = getRecordClass();

      if (recordClass == null) {
        final SerializableSchemaSupplier schemaSupplier =
            new SerializableSchemaSupplier(getSchema());

        return (numBuckets, numShards, hashType) -> {
          try {
            return new AvroBucketMetadata<>(
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
      } else {
        return (numBuckets, numShards, hashType) -> {
          try {
            return new AvroBucketMetadata<>(
                numBuckets,
                numShards,
                keyClass,
                hashType,
                keyField,
                filenamePrefix,
                recordClass);
          } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
            throw new IllegalStateException(e);
          }
        };
      }
    }
  }
}
