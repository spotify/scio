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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.SortedBucketPreKeyedSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.LoggerFactory;

/**
 * Sorted-bucket files are {@code PCollection<V>}s written with {@link SortedBucketSink} that can be
 * efficiently merged without shuffling with {@link SortedBucketSource}. When writing, values are
 * grouped by key into buckets, sorted by key within a bucket, and written to files. When reading,
 * key-values in matching buckets are read in a merge-sort style, reducing shuffle.
 */
public class SortedBucketIO {

  static final int DEFAULT_NUM_BUCKETS = 128;
  static final int DEFAULT_NUM_SHARDS = 1;
  static final HashType DEFAULT_HASH_TYPE = HashType.MURMUR3_128;
  static final int DEFAULT_SORTER_MEMORY_MB = 1024;
  static final String DEFAULT_FILENAME_PREFIX = "bucket";
  static final TargetParallelism DEFAULT_PARALLELISM = TargetParallelism.auto();

  /** Co-groups sorted-bucket sources with the same sort key. */
  public static <FinalKeyT> CoGbkBuilder<FinalKeyT> read(Class<FinalKeyT> finalKeyClass) {
    return new CoGbkBuilder<>(finalKeyClass);
  }

  /** Builder for sorted-bucket {@link CoGbk}. */
  public static class CoGbkBuilder<K> {
    private final Class<K> finalKeyClass;

    private CoGbkBuilder(Class<K> finalKeyClass) {
      this.finalKeyClass = finalKeyClass;
    }

    /** Returns a new {@link CoGbk} with the given first sorted-bucket source in {@link Read}. */
    public CoGbk<K> of(Read<?> read) {
      return new CoGbk<>(
          finalKeyClass,
          Collections.singletonList(read.toBucketedInput()),
          DEFAULT_PARALLELISM,
          null);
    }
  }

  /**
   * A {@link PTransform} for co-grouping sorted-bucket sources using {@link SortedBucketSource}.
   */
  public static class CoGbk<K> extends PTransform<PBegin, PCollection<KV<K, CoGbkResult>>> {
    private final Class<K> keyClass;
    private final List<BucketedInput<?, ?>> inputs;
    private final TargetParallelism targetParallelism;
    private final String metricsKey;

    private CoGbk(
        Class<K> keyClass,
        List<BucketedInput<?, ?>> inputs,
        TargetParallelism targetParallelism,
        String metricsKey) {
      this.keyClass = keyClass;
      this.inputs = inputs;
      this.targetParallelism = targetParallelism;
      this.metricsKey = metricsKey;
    }

    /**
     * Returns a new {@link CoGbk} that is the same as this, appended with the given sorted-bucket
     * source in {@link Read}.
     */
    public CoGbk<K> and(Read<?> read) {
      ImmutableList<BucketedInput<?, ?>> newReads =
          ImmutableList.<BucketedInput<?, ?>>builder()
              .addAll(inputs)
              .add(read.toBucketedInput())
              .build();
      return new CoGbk<>(keyClass, newReads, targetParallelism, metricsKey);
    }

    public CoGbk<K> withTargetParallelism(TargetParallelism targetParallelism) {
      return new CoGbk<>(keyClass, inputs, targetParallelism, metricsKey);
    }

    public CoGbk<K> withMetricsKey(String metricsKey) {
      return new CoGbk<>(keyClass, inputs, targetParallelism, metricsKey);
    }

    public <V> CoGbkTransform<K, V> transform(TransformOutput<K, V> transform) {
      return new CoGbkTransform<>(keyClass, inputs, targetParallelism, transform);
    }

    @Override
    public PCollection<KV<K, CoGbkResult>> expand(PBegin input) {
      SortedBucketSource<K> source;
      if (metricsKey == null) {
        source = new SortedBucketSource<>(keyClass, inputs, targetParallelism);
      } else {
        source = new SortedBucketSource<>(keyClass, inputs, targetParallelism, metricsKey);
      }
      return input.apply(org.apache.beam.sdk.io.Read.from(source));
    }
  }

  public static class CoGbkTransform<K, V> extends PTransform<PBegin, WriteResult> {
    private final Class<K> keyClass;
    private final List<BucketedInput<?, ?>> inputs;
    private final TargetParallelism targetParallelism;
    private TransformFn<K, V> toFinalResultT;

    private final ResourceId outputDirectory;
    private final ResourceId tempDirectory;
    private final NewBucketMetadataFn<K, V> newBucketMetadataFn;
    private final FileOperations<V> fileOperations;
    private final String filenameSuffix;
    private final String filenamePrefix;

    private CoGbkTransform(
        Class<K> keyClass,
        List<BucketedInput<?, ?>> inputs,
        TargetParallelism targetParallelism,
        TransformOutput<K, V> transform) {
      this.keyClass = keyClass;
      this.inputs = inputs;
      this.targetParallelism = targetParallelism;
      this.outputDirectory = transform.getOutputDirectory();
      this.tempDirectory = transform.getTempDirectory();
      this.newBucketMetadataFn = transform.getNewBucketMetadataFn();
      this.fileOperations = transform.getFileOperations();
      this.filenameSuffix = transform.getFilenameSuffix();
      this.filenamePrefix = transform.getFilenamePrefix();
    }

    public CoGbkTransform<K, V> via(TransformFn<K, V> toFinalResultT) {
      this.toFinalResultT = toFinalResultT;
      return this;
    }

    ResourceId getTempDirectoryOrDefault(Pipeline pipeline) {
      if (tempDirectory != null) {
        return tempDirectory;
      }

      final String tempLocationOpt = pipeline.getOptions().getTempLocation();
      LoggerFactory.getLogger(SortedBucketIO.class)
          .info(
              "tempDirectory was not set for SortedBucketTransform, defaulting to {}",
              tempLocationOpt);
      return FileSystems.matchNewResource(tempLocationOpt, true);
    }

    @Override
    public WriteResult expand(PBegin input) {
      Preconditions.checkNotNull(outputDirectory, "outputDirectory is not set");
      Preconditions.checkNotNull(toFinalResultT, "TransformFn<K, V> via() is not set");

      final ResourceId tmpDir = getTempDirectoryOrDefault(input.getPipeline());
      return input.apply(
          new SortedBucketTransform<>(
              keyClass,
              inputs,
              targetParallelism,
              toFinalResultT,
              outputDirectory,
              tmpDir,
              newBucketMetadataFn,
              fileOperations,
              filenameSuffix,
              filenamePrefix));
    }
  }

  public abstract static class TransformOutput<K, V> implements Serializable {
    abstract Class<K> getKeyClass();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract String getFilenamePrefix();

    abstract FileOperations<V> getFileOperations();

    abstract NewBucketMetadataFn<K, V> getNewBucketMetadataFn();
  }

  /** Represents a single sorted-bucket source written using {@link SortedBucketSink}. */
  public abstract static class Read<V> implements Serializable {
    public abstract TupleTag<V> getTupleTag();

    protected abstract BucketedInput<?, V> toBucketedInput();
  }

  public abstract static class DirectWrite<K, V> extends Write<K, V, V> {}

  public abstract static class Write<K, V, T> extends PTransform<PCollection<V>, WriteResult> {
    abstract int getNumBuckets();

    abstract int getNumShards();

    abstract String getFilenamePrefix();

    abstract Class<K> getKeyClass();

    abstract HashType getHashType();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract int getSorterMemoryMb();

    abstract FileOperations<T> getFileOperations();

    abstract BucketMetadata<K, V> getBucketMetadata();

    abstract int getKeyCacheSize();

    @Nullable
    abstract BiFunction<K, Iterable<V>, Iterable<T>> getGroupMappingFn();

    @Nullable
    abstract Coder<T> getOutputValueCoder();

    public PreKeyedWrite<K, V, T> onKeyedCollection(Coder<V> valueCoder, boolean verifyKeyExtraction) {
      return new PreKeyedWrite<K, V, T>(this, valueCoder, verifyKeyExtraction);
    }

    ResourceId getTempDirectoryOrDefault(Pipeline pipeline) {
      if (getTempDirectory() != null) {
        return getTempDirectory();
      }

      final String tempLocationOpt = pipeline.getOptions().getTempLocation();
      LoggerFactory.getLogger(SortedBucketIO.class)
          .info(
              "tempDirectory was not set for SortedBucketSink, defaulting to {}", tempLocationOpt);
      return FileSystems.matchNewResource(tempLocationOpt, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteResult expand(PCollection<V> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");

      return input.apply(
          new SortedBucketSink<K, V, T>(
              getBucketMetadata(),
              getOutputDirectory(),
              getTempDirectoryOrDefault(input.getPipeline()),
              getFilenameSuffix(),
              getFileOperations(),
              getSorterMemoryMb(),
              getKeyCacheSize(),
              getGroupMappingFn(),
              getOutputValueCoder())
              );
    }
  }

  public static class PreKeyedWrite<K, V, T> extends PTransform<PCollection<KV<K, V>>, WriteResult> {
    private final Write<K, V, T> write;
    private final Coder<V> valueCoder;
    private final boolean verifyKeyExtraction;

    public PreKeyedWrite(Write<K, V, T> write, Coder<V> valueCoder, boolean verifyKeyExtraction) {
      this.write = write;
      this.valueCoder = valueCoder;
      this.verifyKeyExtraction = verifyKeyExtraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteResult expand(PCollection<KV<K, V>> input) {
      Preconditions.checkNotNull(write.getOutputDirectory(), "outputDirectory is not set");

      final ResourceId outputDirectory = write.getOutputDirectory();
      ResourceId tempDirectory = write.getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }
      return input.apply(
          new SortedBucketPreKeyedSink<K, V, T>(
              write.getBucketMetadata(),
              outputDirectory,
              tempDirectory,
              write.getFilenameSuffix(),
              write.getFileOperations(),
              write.getSorterMemoryMb(),
              valueCoder,
              verifyKeyExtraction,
              write.getKeyCacheSize()));
    }
  }
}
