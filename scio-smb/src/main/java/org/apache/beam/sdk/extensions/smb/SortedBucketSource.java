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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.PartitionMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;

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
public class SortedBucketSource<FinalKeyT>
    extends PTransform<PBegin, PCollection<KV<FinalKeyT, CoGbkResult>>> {

  public abstract static class TargetParallelism {
    public static MinParallelism MIN = new MinParallelism();
    public static MaxParallelism MAX = new MaxParallelism();

    public static CustomParallelism of(int value) {
      return new CustomParallelism(value);
    }
  }

  private static class MaxParallelism extends TargetParallelism {}

  private static class MinParallelism extends TargetParallelism {}

  private static class CustomParallelism extends TargetParallelism {
    int value;

    CustomParallelism(int value) {
      this.value = value;
    }
  }

  private static final Comparator<byte[]> bytesComparator =
      UnsignedBytes.lexicographicalComparator();

  private final Class<FinalKeyT> finalKeyClass;
  private final transient List<BucketedInput<?, ?>> sources;
  private final TargetParallelism targetParallelism;

  public SortedBucketSource(Class<FinalKeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    this(finalKeyClass, sources, TargetParallelism.of(1024));
  }

  public SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism) {
    this.finalKeyClass = finalKeyClass;
    this.sources = sources;
    this.targetParallelism = targetParallelism;
  }

  @Override
  public final PCollection<KV<FinalKeyT, CoGbkResult>> expand(PBegin begin) {
    final SourceSpec<FinalKeyT> sourceSpec = getSourceSpec(finalKeyClass, sources);
    int parallelism = sourceSpec.getParallelism(targetParallelism);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<Integer> reshuffle = Reshuffle.viaRandomKey();

    final CoGbkResult.CoGbkResultCoder resultCoder =
        CoGbkResult.CoGbkResultCoder.of(
            BucketedInput.schemaOf(sources),
            UnionCoder.of(
                sources.stream().map(BucketedInput::getCoder).collect(Collectors.toList())));

    return begin
        .getPipeline()
        .apply(
            "CreateWorkers",
            Create.of(IntStream.range(0, parallelism).boxed().collect(Collectors.toList()))
                .withCoder(VarIntCoder.of()))
        .apply("ReshuffleKeys", reshuffle)
        .apply(
            "MergeBuckets",
            ParDo.of(new MergeBuckets<>(this.getName(), sources, parallelism, sourceSpec)))
        .setCoder(KvCoder.of(sourceSpec.keyCoder, resultCoder));
  }

  static class SourceSpec<K> {
    int leastNumBuckets;
    int greatestNumBuckets;
    Coder<K> keyCoder;

    SourceSpec(int leastNumBuckets, int greatestNumBuckets, Coder<K> keyCoder) {
      this.leastNumBuckets = leastNumBuckets;
      this.greatestNumBuckets = greatestNumBuckets;
      this.keyCoder = keyCoder;
    }

    int getParallelism(TargetParallelism targetParallelism) {
      int parallelism;
      if (targetParallelism == TargetParallelism.MIN) {
        return leastNumBuckets;
      } else if (targetParallelism == TargetParallelism.MAX) {
        return greatestNumBuckets;
      } else {
        parallelism = ((CustomParallelism) targetParallelism).value;

        Preconditions.checkArgument(
            ((parallelism & parallelism - 1) == 0)
                && parallelism >= leastNumBuckets
                && parallelism <= greatestNumBuckets,
            String.format(
                "Target parallelism must be a power of 2 between the least (%d) and "
                    + "greatest (%d) number of buckets in sources. Was: %d",
                leastNumBuckets, greatestNumBuckets, parallelism));

        return parallelism;
      }
    }
  }

  static <KeyT> SourceSpec<KeyT> getSourceSpec(
      Class<KeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    BucketMetadata<?, ?> first = null;
    Coder<KeyT> finalKeyCoder = null;

    // Check metadata of each source
    for (BucketedInput<?, ?> source : sources) {
      final BucketMetadata<?, ?> current = source.getMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(
            first.isCompatibleWith(current),
            "Source %s is incompatible with source %s",
            sources.get(0),
            source);
      }

      if (current.getKeyClass() == finalKeyClass && finalKeyCoder == null) {
        try {
          @SuppressWarnings("unchecked")
          final Coder<KeyT> coder = (Coder<KeyT>) current.getKeyCoder();
          finalKeyCoder = coder;
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException("Could not provide coder for key class " + finalKeyClass, e);
        } catch (NonDeterministicException e) {
          throw new RuntimeException("Non-deterministic coder for key class " + finalKeyClass, e);
        }
      }
    }

    int leastNumBuckets =
        sources.stream()
            .flatMap(source -> source.getPartitionMetadata().values().stream())
            .map(PartitionMetadata::getNumBuckets)
            .min(Integer::compareTo)
            .get();

    int greatestNumBuckets =
        sources.stream()
            .flatMap(source -> source.getPartitionMetadata().values().stream())
            .map(PartitionMetadata::getNumBuckets)
            .max(Integer::compareTo)
            .get();

    Preconditions.checkNotNull(
        finalKeyCoder, "Could not infer coder for key class %s", finalKeyClass);

    return new SourceSpec<>(leastNumBuckets, greatestNumBuckets, finalKeyCoder);
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBuckets<FinalKeyT> extends DoFn<Integer, KV<FinalKeyT, CoGbkResult>> {
    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final Integer parallelism;
    private final Integer leastNumBuckets;
    private final Coder<FinalKeyT> keyCoder;
    private final List<BucketedInput<?, ?>> sources;

    private final Counter elementsRead;
    private final Distribution keyGroupSize;

    MergeBuckets(
        String transformName,
        List<BucketedInput<?, ?>> sources,
        int targetParallelism,
        SourceSpec<FinalKeyT> sourceSpec) {
      this.parallelism = targetParallelism;
      this.leastNumBuckets = sourceSpec.leastNumBuckets;
      this.keyCoder = sourceSpec.keyCoder;
      this.sources = sources;

      elementsRead = Metrics.counter(SortedBucketSource.class, transformName + "-ElementsRead");
      keyGroupSize =
          Metrics.distribution(SortedBucketSource.class, transformName + "-KeyGroupSize");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      int bucketId = c.element();

      final KeyGroupIterator[] iterators;
      iterators =
          sources.stream()
              .map(i -> i.createIterator(bucketId, parallelism))
              .toArray(KeyGroupIterator[]::new);

      Function<byte[], Boolean> keyGroupFilter;

      if (parallelism.equals(leastNumBuckets)) {
        keyGroupFilter = (bytes) -> true;
      } else {
        keyGroupFilter =
            (bytes) ->
                sources.get(0).getMetadata().rehashBucket(bytes, parallelism) == bucketId;
      }

      merge(
          iterators,
          BucketedInput.schemaOf(sources),
          keyGroupFilter,
          mergedKeyGroup -> {
            try {
              c.output(
                  KV.of(
                      keyCoder.decode(new ByteArrayInputStream(mergedKeyGroup.getKey())),
                      mergedKeyGroup.getValue()));
            } catch (Exception e) {
              throw new RuntimeException("Failed to decode and merge key group", e);
            }
          },
          elementsRead,
          keyGroupSize);
    }

    static void merge(
        KeyGroupIterator[] iterators,
        CoGbkResultSchema resultSchema,
        Function<byte[], Boolean> keyGroupFilter,
        Consumer<KV<byte[], CoGbkResult>> consumer,
        Counter elementsRead,
        Distribution keyGroupSize) {
      final int numSources = iterators.length;

      final TupleTagList tupleTags = resultSchema.getTupleTagList();
      final Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        int completedSources = 0;
        // Advance key-value groups from each source
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
          break;
        }

        // Find next key-value groups
        final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry =
            nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);

        final boolean acceptKeyGroup = keyGroupFilter.apply(minKeyEntry.getValue().getKey());

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
            if (acceptKeyGroup) {
              int index = resultSchema.getIndex(entry.getKey());
              @SuppressWarnings("unchecked") final List<Object> values = (List<Object>) valueMap
                  .get(index);
              // TODO: this exhausts everything from the "lazy" iterator and can be expensive.
              // To fix we have to make the underlying Reader range aware so that it's safe to
              // re-iterate or stop without exhausting remaining elements in the value group.
              entry.getValue().getValue().forEachRemaining(values::add);

              nextKeyGroupsIt.remove();
              keyGroupCount += values.size();
            } else {
              // Still have to exhaust iterator
              entry.getValue().getValue().forEachRemaining(value -> {});
              nextKeyGroupsIt.remove();
            }
          }
        }

        if (acceptKeyGroup) {
          keyGroupSize.update(keyGroupCount);
          elementsRead.inc(keyGroupCount);

          final KV<byte[], CoGbkResult> mergedKeyGroup =
              KV.of(
                  minKeyEntry.getValue().getKey(),
                  CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
          consumer.accept(mergedKeyGroup);
        }

        if (completedSources == numSources) {
          break;
        }
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("keyCoder", keyCoder.getClass()));
      builder.add(DisplayData.item("parallelism", parallelism));
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

    public Map<ResourceId, PartitionMetadata> getPartitionMetadata() {
      return getOrComputeMetadata().getPartitionMetadata();
    }

    private SourceMetadata<K, V> getOrComputeMetadata() {
      if (sourceMetadata == null) {
        sourceMetadata =
            BucketMetadataUtil.get().getSourceMetadata(inputDirectories, filenameSuffix);
      }
      return sourceMetadata;
    }

    KeyGroupIterator<byte[], V> createIterator(int workerId, int targetParallelism) {
      final List<Iterator<V>> iterators = new ArrayList<>();
      // Create one iterator per shard
      getOrComputeMetadata()
          .getPartitionMetadata()
          .forEach(
              (resourceId, partitionMetadata) -> {
                final int numBuckets = partitionMetadata.getNumBuckets();
                final int numShards = partitionMetadata.getNumShards();

                // Since all BucketedInputs have a bucket count that's a power of two, we can infer
                // which buckets should be merged together for the join.
                for (int i = (workerId % numBuckets); i < numBuckets; i += targetParallelism) {
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
