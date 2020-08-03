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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.tensorflow.example.Example;

/**
 * API for reading and writing sorted-bucket TensorFlow TFRecord files with TensorFlow {@link
 * Example} records.
 */
public class TensorFlowBucketIO {
  private static final String DEFAULT_SUFFIX = ".tfrecord";

  /**
   * Returns a new {@link Read} for TensorFlow TFRecord files with TensorFlow {@link Example}
   * records.
   */
  public static Read read(TupleTag<Example> tupleTag) {
    return new AutoValue_TensorFlowBucketIO_Read.Builder()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.AUTO)
        .build();
  }

  /**
   * Returns a new {@link Write} for TensorFlow TFRecord files with TensorFlow {@link Example}
   * records.
   */
  public static <K> Write<K> write(Class<K> keyClass, String keyField) {
    return new AutoValue_TensorFlowBucketIO_Write.Builder<K>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCompression(Compression.UNCOMPRESSED)
        .build();
  }

  /** Returns a new {@link TransformOutput} for Avro generic records. */
  public static <K> TransformOutput<K> transformOutput(Class<K> keyClass, String keyField) {
    return new AutoValue_TensorFlowBucketIO_TransformOutput.Builder<K>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.UNCOMPRESSED)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Reads from sorted-bucket TensorFlow TFRecord files with TensorFlow {@link Example} records, to
   * be used with {@link SortedBucketIO.CoGbk}.
   */
  @AutoValue
  public abstract static class Read extends SortedBucketIO.Read<Example> {
    @Nullable
    abstract ImmutableList<ResourceId> getInputDirectories();

    abstract String getFilenameSuffix();

    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTupleTag(TupleTag<Example> tupleTag);

      abstract Builder setInputDirectories(ResourceId... inputDirectories);

      abstract Builder setInputDirectories(List<ResourceId> inputDirectories);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setCompression(Compression compression);

      abstract Read build();
    }

    /** Reads from the given input directory. */
    public Read from(String inputDirectory) {
      return toBuilder()
          .setInputDirectories(FileSystems.matchNewResource(inputDirectory, true))
          .build();
    }

    /** Specifies the input filename suffix. */
    public Read withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    @Override
    protected BucketedInput<?, Example> toBucketedInput() {
      return new BucketedInput<>(
          getTupleTag(),
          getInputDirectories(),
          getFilenameSuffix(),
          TensorFlowFileOperations.of(getCompression()));
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Writes to sorted-bucket TensorFlow TFRecord files with TensorFlow {@link Example} records with
   * {@link SortedBucketSink}.
   */
  @AutoValue
  public abstract static class Write<K> extends SortedBucketIO.Write<K, Example> {
    abstract int getSorterMemoryMb();

    // TFRecord specific
    @Nullable
    abstract String getKeyField();

    abstract Compression getCompression();

    abstract Builder<K> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K> {
      // Common

      abstract Builder<K> setNumBuckets(int numBuckets);

      abstract Builder<K> setNumShards(int numShards);

      abstract Builder<K> setKeyClass(Class<K> keyClass);

      abstract Builder<K> setHashType(HashType hashType);

      abstract Builder<K> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K> setFilenamePrefix(String filenamePrefix);

      abstract Builder<K> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K> setSorterMemoryMb(int sorterMemoryMb);

      // TFRecord specific
      abstract Builder<K> setKeyField(String keyField);

      abstract Builder<K> setCompression(Compression compression);

      abstract Builder<K> setKeyCacheSize(int cacheSize);

      abstract Write<K> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. */
    public Write<K> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public Write<K> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix (i.e. "bucket" or "part"). */
    public Write<K> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the size of an optional key-to-hash cache in the ExtractKeys transform. */
    public Write<K> withKeyCacheOfSize(int keyCacheSize) {
      return toBuilder().setKeyCacheSize(keyCacheSize).build();
    }

    /** Specifies the output file {@link Compression}. */
    public Write<K> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    FileOperations<Example> getFileOperations() {
      return TensorFlowFileOperations.of(getCompression());
    }

    @Override
    BucketMetadata<K, Example> getBucketMetadata() {
      try {
        return new TensorFlowBucketMetadata<>(
            getNumBuckets(),
            getNumShards(),
            getKeyClass(),
            getHashType(),
            getKeyField(),
            getFilenamePrefix());
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Writes to sorted-bucket TensorFlow TFRecord files with TensorFlow {@link Example} records with
   * {@link SortedBucketTransform}.
   */
  @AutoValue
  public abstract static class TransformOutput<K>
      extends SortedBucketIO.TransformOutput<K, Example> {

    // JSON specific
    @Nullable
    abstract String getKeyField();

    abstract Compression getCompression();

    abstract Builder<K> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K> {

      // Common
      abstract Builder<K> setKeyClass(Class<K> keyClass);

      abstract Builder<K> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K> setFilenamePrefix(String filenamePrefix);

      // JSON specific
      abstract Builder<K> setKeyField(String keyField);

      abstract Builder<K> setCompression(Compression compression);

      abstract TransformOutput<K> build();
    }

    /** Writes to the given output directory. */
    public TransformOutput<K> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. */
    public TransformOutput<K> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public TransformOutput<K> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix. */
    public TransformOutput<K> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the output file {@link Compression}. */
    public TransformOutput<K> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    FileOperations<Example> getFileOperations() {
      return TensorFlowFileOperations.of(getCompression());
    }

    @Override
    NewBucketMetadataFn<K, Example> getNewBucketMetadataFn() {
      final String keyField = getKeyField();
      final Class<K> keyClass = getKeyClass();
      final String filenamePrefix = getFilenamePrefix();

      return (numBuckets, numShards, hashType) -> {
        try {
          return new TensorFlowBucketMetadata<>(
              numBuckets, numShards, keyClass, hashType, keyField, filenamePrefix);
        } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
          throw new IllegalStateException(e);
        }
      };
    }
  }
}
