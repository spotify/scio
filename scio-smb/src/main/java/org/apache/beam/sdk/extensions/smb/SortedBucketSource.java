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

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.PartitionMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for co-grouping sources written using compatible {@link SortedBucketSink}
 * transforms. It differs from {@link org.apache.beam.sdk.transforms.join.CoGroupByKey} because no
 * shuffle step is required, since the source files are written in pre-sorted order. Instead,
 * matching buckets' files are sequentially read in a merge-sort style, and outputs resulting value
 * groups as {@link org.apache.beam.sdk.transforms.join.CoGbkResult}.
 *
 * <h3>Source compatibility</h3>
 *
 * <p>Each of the {@link BucketedInput} sources must use the same key function and hashing scheme.
 * Since {@link SortedBucketSink} writes an additional file representing {@link BucketMetadata},
 * {@link SortedBucketSource} begins by reading each metadata file and using {@link
 * BucketMetadata#isCompatibleWith(BucketMetadata)} to check compatibility.
 *
 * <p>The number of buckets, {@code N}, does not have to match across sources. Since that value is
 * required be to a power of 2, all values of {@code N} are compatible, albeit requiring a fan-out
 * from the source with smallest {@code N}.
 *
 * @param <FinalKeyT> the type of the result keys. Sources can have different key types as long as
 *     they can all be decoded as this type (see: {@link BucketMetadata#getKeyCoder()} and are
 *     bucketed using the same {@code byte[]} representation (see: {@link
 *     BucketMetadata#getKeyBytes(Object)}.
 */
public class SortedBucketSource<FinalKeyT> extends BoundedSource<KV<FinalKeyT, CoGbkResult>> {

  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.33;

  private static final Comparator<byte[]> bytesComparator =
      UnsignedBytes.lexicographicalComparator();

  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSource.class);

  private final Class<FinalKeyT> finalKeyClass;
  private final List<BucketedInput<?, ?>> sources;
  private final TargetParallelism targetParallelism;
  private final int effectiveParallelism;
  private final int bucketOffsetId;
  private SourceSpec<FinalKeyT> sourceSpec;
  private final Distribution keyGroupSize;
  private Long estimatedSizeBytes;

  public SortedBucketSource(Class<FinalKeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    this(finalKeyClass, sources, TargetParallelism.min());
  }

  public SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(finalKeyClass, sources, targetParallelism, 0, 1);
  }

  private SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism) {
    this.finalKeyClass = finalKeyClass;
    this.sources = sources;
    this.targetParallelism = targetParallelism;
    this.bucketOffsetId = bucketOffsetId;
    this.effectiveParallelism = effectiveParallelism;
    this.keyGroupSize =
        Metrics.distribution(SortedBucketSource.class, "SortedBucketSource-KeyGroupSize");
  }

  @VisibleForTesting
  int getBucketOffset() {
    return bucketOffsetId;
  }

  private SourceSpec<FinalKeyT> getOrComputeSourceSpec() {
    if (this.sourceSpec == null) {
      this.sourceSpec = SourceSpec.from(finalKeyClass, sources);
    }
    return this.sourceSpec;
  }

  @Override
  public Coder<KV<FinalKeyT, CoGbkResult>> getOutputCoder() {
    return KvCoder.of(
        getOrComputeSourceSpec().keyCoder,
        CoGbkResult.CoGbkResultCoder.of(
            BucketedInput.schemaOf(sources),
            UnionCoder.of(
                sources.stream().map(BucketedInput::getCoder).collect(Collectors.toList()))));
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
    builder.add(DisplayData.item("keyClass", finalKeyClass.toString()));
  }

  // `getEstimatedSizeBytes` is called frequently by Dataflow, don't recompute every time
  private long getOrComputeSizeBytes() throws Exception {
    if (estimatedSizeBytes == null) {
      estimatedSizeBytes =
          FileSystems.matchResources(
                  sources.stream()
                      .flatMap(
                          s -> s.listFilesForBucket(bucketOffsetId, effectiveParallelism).stream())
                      .collect(Collectors.toList()))
              .stream()
              .flatMap(
                  matchResult -> {
                    try {
                      return matchResult.metadata().stream().map(Metadata::sizeBytes);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .reduce(0L, Long::sum);
    }

    return estimatedSizeBytes;
  }

  @Override
  public List<? extends BoundedSource<KV<FinalKeyT, CoGbkResult>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

    int greatestNumBuckets = getOrComputeSourceSpec().greatestNumBuckets;

    if (effectiveParallelism == greatestNumBuckets) {
      LOG.info("Parallelism is already maxed, can't split further.");
      return ImmutableList.of(this);
    }
    int parallelism;

    if (!targetParallelism.isAuto()) {
      parallelism = getOrComputeSourceSpec().getParallelism(targetParallelism);
    } else {
      long size = getEstimatedSizeBytes(options);
      int fanout = (int) (size / (DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR * desiredBundleSizeBytes));

      if (fanout <= 1) {
        return ImmutableList.of(this);
      }

      // round up to nearest power of 2, bounded by greatest # of buckets
      parallelism = Math.min(Integer.highestOneBit(fanout - 1) * 2, greatestNumBuckets);
    }

    final int effectiveParallelism = parallelism;
    LOG.info("Parallelism was adjusted to " + effectiveParallelism);

    return IntStream.range(0, effectiveParallelism)
        .boxed()
        .map(
            i ->
                new SortedBucketSource<>(
                    finalKeyClass, sources, targetParallelism, i, effectiveParallelism))
        .collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return getOrComputeSizeBytes();
  }

  @Override
  public BoundedReader<KV<FinalKeyT, CoGbkResult>> createReader(PipelineOptions options)
      throws IOException {
    return new MergeBucketsReader<>(
        sources,
        bucketOffsetId,
        effectiveParallelism,
        getOrComputeSourceSpec(),
        this,
        keyGroupSize);
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBucketsReader<FinalKeyT> extends BoundedReader<KV<FinalKeyT, CoGbkResult>> {
    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final Coder<FinalKeyT> keyCoder;
    private final SortedBucketSource<FinalKeyT> currentSource;
    private final Distribution keyGroupSize;
    private final int numSources;
    private final KeyGroupIterator[] iterators;
    private final Function<byte[], Boolean> keyGroupFilter;
    private final CoGbkResultSchema resultSchema;
    private final TupleTagList tupleTags;

    private Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups;
    private Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry;

    MergeBucketsReader(
        List<BucketedInput<?, ?>> sources,
        Integer bucketId,
        int parallelism,
        SourceSpec<FinalKeyT> sourceSpec,
        SortedBucketSource<FinalKeyT> currentSource,
        Distribution keyGroupSize) {
      this.keyCoder = sourceSpec.keyCoder;
      this.numSources = sources.size();
      this.currentSource = currentSource;
      this.keyGroupSize = keyGroupSize;

      if (parallelism == sourceSpec.leastNumBuckets) {
        this.keyGroupFilter = (bytes) -> true;
      } else {
        this.keyGroupFilter =
            sources.get(0).getMetadata().checkRehashedBucketFn(parallelism, bucketId);
      }

      iterators =
          sources.stream()
              .map(i -> i.createIterator(bucketId, parallelism))
              .toArray(KeyGroupIterator[]::new);

      resultSchema = BucketedInput.schemaOf(sources);
      tupleTags = resultSchema.getTupleTagList();
    }

    @Override
    public boolean start() throws IOException {
      nextKeyGroups = new HashMap<>();
      return advance();
    }

    @Override
    public KV<FinalKeyT, CoGbkResult> getCurrent() throws NoSuchElementException {
      // Find next key-value groups
      final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
          nextKeyGroups.entrySet().iterator();
      final List<Iterable<?>> valueMap = new ArrayList<>();
      for (int i = 0; i < resultSchema.size(); i++) {
        valueMap.add(new ArrayList<>());
      }

      int keyGroupCount = 0;
      while (nextKeyGroupsIt.hasNext()) {
        final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> entry = nextKeyGroupsIt.next();
        if (keyComparator.compare(entry, minKeyEntry) == 0) {
          int index = resultSchema.getIndex(entry.getKey());
          @SuppressWarnings("unchecked")
          final List<Object> values = (List<Object>) valueMap.get(index);
          // TODO: this exhausts everything from the "lazy" iterator and can be expensive.
          // To fix we have to make the underlying Reader range aware so that it's safe to
          // re-iterate or stop without exhausting remaining elements in the value group.
          entry.getValue().getValue().forEachRemaining(values::add);

          nextKeyGroupsIt.remove();
          keyGroupCount += values.size();
        }
      }

      keyGroupSize.update(keyGroupCount);
      final KV<byte[], CoGbkResult> mergedKeyGroup =
          KV.of(
              minKeyEntry.getValue().getKey(),
              CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
      try {
        return KV.of(
            keyCoder.decode(new ByteArrayInputStream(mergedKeyGroup.getKey())),
            mergedKeyGroup.getValue());
      } catch (Exception e) {
        throw new RuntimeException("Failed to decode and merge key group", e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      int completedSources = 0;

      // Advance key-value groups from each source
      do {
        for (int i = 0; i < numSources; i++) {
          final KeyGroupIterator it = iterators[i];
          if (nextKeyGroups.containsKey(tupleTags.get(i))) {
            continue;
          }
          if (it.hasNext()) {
            @SuppressWarnings("unchecked")
            final KV<byte[], Iterator<?>> next = it.next();
            nextKeyGroups.put(tupleTags.get(i), next);
          } else {
            completedSources++;
          }
        }

        if (nextKeyGroups.isEmpty()) {
          return false;
        }
        minKeyEntry = nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);

        if (keyGroupFilter.apply(minKeyEntry.getValue().getKey())) {
          return true;
        } else {
          // Still have to exhaust iterator

          final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
              nextKeyGroups.entrySet().iterator();
          final List<Iterable<?>> valueMap = new ArrayList<>();
          for (int i = 0; i < resultSchema.size(); i++) {
            valueMap.add(new ArrayList<>());
          }

          while (nextKeyGroupsIt.hasNext()) {
            final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> entry = nextKeyGroupsIt.next();
            if (keyComparator.compare(entry, minKeyEntry) == 0) {
              entry.getValue().getValue().forEachRemaining(value -> {});
              nextKeyGroupsIt.remove();
            }
          }
        }
      } while (completedSources != numSources);

      return !nextKeyGroups.isEmpty();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<KV<FinalKeyT, CoGbkResult>> getCurrentSource() {
      return currentSource;
    }
  }

  /**
   * Abstracts a sorted-bucket input to {@link SortedBucketSource} written by {@link
   * SortedBucketSink}.
   *
   * @param <K> the type of the keys that values in a bucket are sorted with
   * @param <V> the type of the values in a bucket
   */
  public static class BucketedInput<K, V> implements Serializable {
    private TupleTag<V> tupleTag;
    private String filenameSuffix;
    private FileOperations<V> fileOperations;
    private List<ResourceId> inputDirectories;

    private transient SourceMetadata<K, V> sourceMetadata;

    public BucketedInput(
        TupleTag<V> tupleTag,
        ResourceId inputDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations) {
      this(tupleTag, Collections.singletonList(inputDirectory), filenameSuffix, fileOperations);
    }

    public BucketedInput(
        TupleTag<V> tupleTag,
        List<ResourceId> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations) {
      this.tupleTag = tupleTag;
      this.filenameSuffix = filenameSuffix;
      this.fileOperations = fileOperations;
      this.inputDirectories = inputDirectories;
    }

    public TupleTag<V> getTupleTag() {
      return tupleTag;
    }

    public Coder<V> getCoder() {
      return fileOperations.getCoder();
    }

    static CoGbkResultSchema schemaOf(List<BucketedInput<?, ?>> sources) {
      return CoGbkResultSchema.of(
          sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList()));
    }

    public BucketMetadata<K, V> getMetadata() {
      return getOrComputeMetadata().getCanonicalMetadata();
    }

    Map<ResourceId, PartitionMetadata> getPartitionMetadata() {
      return getOrComputeMetadata().getPartitionMetadata();
    }

    private SourceMetadata<K, V> getOrComputeMetadata() {
      if (sourceMetadata == null) {
        sourceMetadata =
            BucketMetadataUtil.get().getSourceMetadata(inputDirectories, filenameSuffix);
      }
      return sourceMetadata;
    }

    List<ResourceId> listFilesForBucket(int bucketId, int targetParallelism) {
      final List<ResourceId> files = new ArrayList<>();
      // Create one iterator per shard
      getPartitionMetadata()
          .forEach(
              (resourceId, partitionMetadata) -> {
                final int numBuckets = partitionMetadata.getNumBuckets();
                final int numShards = partitionMetadata.getNumShards();

                // Since all BucketedInputs have a bucket count that's a power of two, we can infer
                // which buckets should be merged together for the join.
                for (int i = (bucketId % numBuckets); i < numBuckets; i += targetParallelism) {
                  for (int j = 0; j < numShards; j++) {
                    files.add(
                        partitionMetadata
                            .getFileAssignment()
                            .forBucket(BucketShardId.of(i, j), numBuckets, numShards));
                  }
                }
              });

      return files;
    }

    KeyGroupIterator<byte[], V> createIterator(int bucketId, int targetParallelism) {
      final List<Iterator<V>> iterators = new ArrayList<>();
      // Create one iterator per shard
      getPartitionMetadata()
          .forEach(
              (resourceId, partitionMetadata) -> {
                final int numBuckets = partitionMetadata.getNumBuckets();
                final int numShards = partitionMetadata.getNumShards();

                // Since all BucketedInputs have a bucket count that's a power of two, we can infer
                // which buckets should be merged together for the join.
                for (int i = (bucketId % numBuckets); i < numBuckets; i += targetParallelism) {
                  for (int j = 0; j < numShards; j++) {
                    final ResourceId file =
                        partitionMetadata
                            .getFileAssignment()
                            .forBucket(BucketShardId.of(i, j), numBuckets, numShards);
                    try {
                      iterators.add(fileOperations.iterator(file));
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                }
              });

      BucketMetadata<K, V> canonicalMetadata = sourceMetadata.getCanonicalMetadata();
      return new KeyGroupIterator<>(iterators, canonicalMetadata::getKeyBytes, bytesComparator);
    }

    @Override
    public String toString() {
      List<ResourceId> inputDirectories =
          new ArrayList<>(getOrComputeMetadata().getPartitionMetadata().keySet());

      return String.format(
          "BucketedInput[tupleTag=%s, inputDirectories=[%s], metadata=%s]",
          tupleTag.getId(),
          inputDirectories.size() > 5
              ? inputDirectories.subList(0, 4)
                  + "..."
                  + inputDirectories.get(inputDirectories.size() - 1)
              : inputDirectories,
          sourceMetadata.getCanonicalMetadata());
    }

    // Not all instance members can be natively serialized, so override writeObject/readObject
    // using Coders for each type
    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream outStream) throws IOException {
      SerializableCoder.of(TupleTag.class).encode(tupleTag, outStream);
      StringUtf8Coder.of().encode(filenameSuffix, outStream);
      SerializableCoder.of(FileOperations.class).encode(fileOperations, outStream);

      // Depending on when .writeObject is called, metadata may not have been computed.
      NullableCoder.of(SerializableCoder.of(SourceMetadata.class))
          .encode(sourceMetadata, outStream);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inStream) throws ClassNotFoundException, IOException {
      this.tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
      this.filenameSuffix = StringUtf8Coder.of().decode(inStream);
      this.fileOperations = SerializableCoder.of(FileOperations.class).decode(inStream);

      this.sourceMetadata =
          NullableCoder.of(SerializableCoder.of(SourceMetadata.class)).decode(inStream);
    }
  }
}
