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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

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
      return new CoGbk<>(finalKeyClass, Collections.singletonList(read), DEFAULT_PARALLELISM, null);
    }
  }

  /**
   * A {@link PTransform} for co-grouping sorted-bucket sources using {@link SortedBucketSource}.
   */
  public static class CoGbk<K> extends PTransform<PBegin, PCollection<KV<K, CoGbkResult>>> {
    private final Class<K> keyClass;
    private final List<Read<?>> reads;
    private final TargetParallelism targetParallelism;
    private final String metricsKey;

    private CoGbk(
        Class<K> keyClass,
        List<Read<?>> reads,
        TargetParallelism targetParallelism,
        String metricsKey) {
      this.keyClass = keyClass;
      this.reads = reads;
      this.targetParallelism = targetParallelism;
      this.metricsKey = metricsKey;
    }

    /**
     * Returns a new {@link CoGbk} that is the same as this, appended with the given sorted-bucket
     * source in {@link Read}.
     */
    public CoGbk<K> and(Read<?> read) {
      ImmutableList<Read<?>> newReads =
          ImmutableList.<Read<?>>builder().addAll(reads).add(read).build();
      return new CoGbk<>(keyClass, newReads, targetParallelism, metricsKey);
    }

    public CoGbk<K> withTargetParallelism(TargetParallelism targetParallelism) {
      return new CoGbk<>(keyClass, reads, targetParallelism, metricsKey);
    }

    public CoGbk<K> withMetricsKey(String metricsKey) {
      return new CoGbk<>(keyClass, reads, targetParallelism, metricsKey);
    }

    public <V> CoGbkTransform<K, V> to(SortedBucketIO.Write<K, V> write) {
      return new CoGbkTransform<>(this.keyClass, this.reads, write);
    }

    @Override
    public PCollection<KV<K, CoGbkResult>> expand(PBegin input) {
      final List<BucketedInput<?, ?>> bucketedInputs =
          reads.stream().map(Read::toBucketedInput).collect(Collectors.toList());
      SortedBucketSource<K> source;
      if (metricsKey == null) {
        source = new SortedBucketSource<>(keyClass, bucketedInputs, targetParallelism);
      } else {
        source = new SortedBucketSource<>(keyClass, bucketedInputs, targetParallelism, metricsKey);
      }
      return input.apply(org.apache.beam.sdk.io.Read.from(source));
    }
  }

  public static class CoGbkTransform<K, V> extends PTransform<PBegin, WriteResult> {
    private final Class<K> keyClass;
    private final List<Read<?>> reads;
    private final SortedBucketIO.Write<K, V> write;
    private TransformFn<K, V> toFinalResultT;

    private CoGbkTransform(
        Class<K> keyClass, List<Read<?>> reads, SortedBucketIO.Write<K, V> write) {
      this.keyClass = keyClass;
      this.reads = reads;
      this.write = write;
    }

    public CoGbkTransform<K, V> via(TransformFn<K, V> toFinalResultT) {
      this.toFinalResultT = toFinalResultT;
      return this;
    }

    @Override
    public WriteResult expand(PBegin input) {
      Preconditions.checkNotNull(write.getOutputDirectory(), "outputDirectory is not set");
      Preconditions.checkNotNull(toFinalResultT, "TransformFn<K, V>v via() is not set");

      final List<BucketedInput<?, ?>> bucketedInputs =
          reads.stream().map(Read::toBucketedInput).collect(Collectors.toList());

      final ResourceId outputDirectory = write.getOutputDirectory();
      ResourceId tempDirectory = write.getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }

      return input.apply(
          new SortedBucketTransform<>(
              keyClass,
              TargetParallelism.auto(),
              outputDirectory,
              tempDirectory,
              write.getFilenameSuffix(),
              write.getFileOperations(),
              bucketedInputs,
              toFinalResultT,
              ((numBuckets, numShards) -> write.getBucketMetadata())));
    }
  }

  /** Represents a single sorted-bucket source written using {@link SortedBucketSink}. */
  public abstract static class Read<V> {
    public abstract TupleTag<V> getTupleTag();

    protected abstract BucketedInput<?, V> toBucketedInput();
  }

  public abstract static class Write<K, V> extends PTransform<PCollection<V>, WriteResult> {
    abstract int getNumBuckets();

    abstract int getNumShards();

    abstract Class<K> getKeyClass();

    abstract HashType getHashType();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract int getSorterMemoryMb();

    abstract FileOperations<V> getFileOperations();

    abstract BucketMetadata<K, V> getBucketMetadata();

    @Override
    public WriteResult expand(PCollection<V> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");

      final ResourceId outputDirectory = getOutputDirectory();
      ResourceId tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }

      return input.apply(
          new SortedBucketSink<>(
              getBucketMetadata(),
              outputDirectory,
              tempDirectory,
              getFilenameSuffix(),
              getFileOperations(),
              getSorterMemoryMb()));
    }
  }
}
