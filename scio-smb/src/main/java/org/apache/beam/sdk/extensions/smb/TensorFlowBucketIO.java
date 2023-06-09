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
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.tensorflow.proto.example.Example;

/**
 * API for reading and writing sorted-bucket TensorFlow TFRecord files with TensorFlow {@link
 * Example} records.
 */
public class TensorFlowBucketIO {
  private static final String DEFAULT_SUFFIX = ".tfrecord";

  // make sure tensorflow is part of the classpath
  static {
    try {
      Class.forName("org.tensorflow.proto.example.Example");
    } catch (ClassNotFoundException e) {
      throw new MissingImplementationException("tensorflow", e);
    }
  }

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
  public static <K1> Write<K1, Void> write(Class<K1> keyClassPrimary, String keyFieldPrimary) {
    return write(keyClassPrimary, keyFieldPrimary, null, null);
  }

  /**
   * Returns a new {@link Write} for TensorFlow TFRecord files with TensorFlow {@link Example}
   * records.
   */
  public static <K1, K2> Write<K1, K2> write(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary) {
    return new AutoValue_TensorFlowBucketIO_Write.Builder<K1, K2>()
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClassPrimary(keyClassPrimary)
        .setKeyClassSecondary(keyClassSecondary)
        .setKeyFieldPrimary(keyFieldPrimary)
        .setKeyFieldSecondary(keyFieldSecondary)
        .setKeyCacheSize(0)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setCompression(Compression.UNCOMPRESSED)
        .build();
  }

  public static <K1> TransformOutput<K1, Void> transformOutput(
      Class<K1> keyClassPrimary, String keyFieldPrimary) {
    return transformOutput(keyClassPrimary, keyFieldPrimary, null, null);
  }

  public static <K1, K2> TransformOutput<K1, K2> transformOutput(
      Class<K1> keyClassPrimary,
      String keyFieldPrimary,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary) {
    return new AutoValue_TensorFlowBucketIO_TransformOutput.Builder<K1, K2>()
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setFilenamePrefix(SortedBucketIO.DEFAULT_FILENAME_PREFIX)
        .setKeyClassPrimary(keyClassPrimary)
        .setKeyClassSecondary(keyClassSecondary)
        .setKeyFieldPrimary(keyFieldPrimary)
        .setKeyFieldSecondary(keyFieldSecondary)
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
    abstract ImmutableList<String> getInputDirectories();

    abstract String getFilenameSuffix();

    abstract Compression getCompression();

    @Nullable
    abstract Predicate<Example> getPredicate();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTupleTag(TupleTag<Example> tupleTag);

      abstract Builder setInputDirectories(String... inputDirectories);

      abstract Builder setInputDirectories(List<String> inputDirectories);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setCompression(Compression compression);

      abstract Builder setPredicate(Predicate<Example> predicate);

      abstract Read build();
    }

    /** Reads from the given input directory. */
    public Read from(String inputDirectory) {
      return toBuilder().setInputDirectories(inputDirectory).build();
    }

    /** Specifies the input filename suffix. */
    public Read withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the filter predicate. */
    public Read withPredicate(Predicate<Example> predicate) {
      return toBuilder().setPredicate(predicate).build();
    }

    @Override
    protected BucketedInput<Example> toBucketedInput(final SortedBucketSource.Keying keying) {
      return BucketedInput.of(
          keying,
          getTupleTag(),
          getInputDirectories(),
          getFilenameSuffix(),
          TensorFlowFileOperations.of(getCompression()),
          getPredicate());
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
  public abstract static class Write<K1, K2> extends SortedBucketIO.Write<K1, K2, Example> {
    abstract int getSorterMemoryMb();

    // TFRecord specific
    @Nullable
    abstract String getKeyFieldPrimary();

    abstract String getKeyFieldSecondary();

    abstract Compression getCompression();

    abstract Builder<K1, K2> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K1, K2> {
      // Common

      abstract Builder<K1, K2> setNumBuckets(int numBuckets);

      abstract Builder<K1, K2> setNumShards(int numShards);

      abstract Builder<K1, K2> setKeyClassPrimary(Class<K1> keyClassPrimary);

      abstract Builder<K1, K2> setKeyClassSecondary(Class<K2> keyClassSecondary);

      abstract Builder<K1, K2> setHashType(HashType hashType);

      abstract Builder<K1, K2> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K1, K2> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K1, K2> setFilenamePrefix(String filenamePrefix);

      abstract Builder<K1, K2> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K1, K2> setSorterMemoryMb(int sorterMemoryMb);

      // TFRecord specific
      abstract Builder<K1, K2> setKeyFieldPrimary(String keyFieldPrimary);

      abstract Builder<K1, K2> setKeyFieldSecondary(String keyFieldSecondary);

      abstract Builder<K1, K2> setCompression(Compression compression);

      abstract Builder<K1, K2> setKeyCacheSize(int cacheSize);

      abstract Write<K1, K2> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K1, K2> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K1, K2> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K1, K2> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K1, K2> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public Write<K1, K2> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public Write<K1, K2> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix (i.e. "bucket" or "part"). */
    public Write<K1, K2> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K1, K2> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the size of an optional key-to-hash cache in the ExtractKeys transform. */
    public Write<K1, K2> withKeyCacheOfSize(int keyCacheSize) {
      return toBuilder().setKeyCacheSize(keyCacheSize).build();
    }

    /** Specifies the output file {@link Compression}. */
    public Write<K1, K2> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    FileOperations<Example> getFileOperations() {
      return TensorFlowFileOperations.of(getCompression());
    }

    @Override
    BucketMetadata<K1, K2, Example> getBucketMetadata() {
      try {
        return new TensorFlowBucketMetadata<K1, K2>(
            getNumBuckets(),
            getNumShards(),
            getKeyClassPrimary(),
            getKeyFieldPrimary(),
            getKeyClassSecondary(),
            getKeyFieldSecondary(),
            getHashType(),
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
  public abstract static class TransformOutput<K1, K2>
      extends SortedBucketIO.TransformOutput<K1, K2, Example> {

    // JSON specific
    @Nullable
    abstract String getKeyFieldPrimary();

    @Nullable
    abstract String getKeyFieldSecondary();

    abstract Compression getCompression();

    abstract Builder<K1, K2> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K1, K2> {

      // Common
      abstract Builder<K1, K2> setKeyClassPrimary(Class<K1> keyClassPrimary);

      abstract Builder<K1, K2> setKeyClassSecondary(Class<K2> keyClassSecondary);

      abstract Builder<K1, K2> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K1, K2> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K1, K2> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K1, K2> setFilenamePrefix(String filenamePrefix);

      // JSON specific
      abstract Builder<K1, K2> setKeyFieldPrimary(String keyFieldPrimary);

      abstract Builder<K1, K2> setKeyFieldSecondary(String keyFieldSecondary);

      abstract Builder<K1, K2> setCompression(Compression compression);

      abstract TransformOutput<K1, K2> build();
    }

    /** Writes to the given output directory. */
    public TransformOutput<K1, K2> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. Defaults to --tempLocation if not set. */
    public TransformOutput<K1, K2> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public TransformOutput<K1, K2> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the output filename prefix. */
    public TransformOutput<K1, K2> withFilenamePrefix(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Specifies the output file {@link Compression}. */
    public TransformOutput<K1, K2> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    FileOperations<Example> getFileOperations() {
      return TensorFlowFileOperations.of(getCompression());
    }

    @Override
    NewBucketMetadataFn<K1, K2, Example> getNewBucketMetadataFn() {
      final String keyFieldPrimary = getKeyFieldPrimary();
      final String keyFieldSecondary = getKeyFieldSecondary();
      final Class<K1> keyClassPrimary = getKeyClassPrimary();
      final Class<K2> keyClassSecondary = getKeyClassSecondary();
      final String filenamePrefix = getFilenamePrefix();

      return (numBuckets, numShards, hashType) -> {
        try {
          return new TensorFlowBucketMetadata<K1, K2>(
              numBuckets,
              numShards,
              keyClassPrimary,
              keyFieldPrimary,
              keyClassSecondary,
              keyFieldSecondary,
              hashType,
              filenamePrefix);
        } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
          throw new IllegalStateException(e);
        }
      };
    }
  }
}
