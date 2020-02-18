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
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.BucketMetadataCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.UnmodifiableIterator;
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

  private static final Comparator<byte[]> bytesComparator =
      UnsignedBytes.lexicographicalComparator();

  private final Class<FinalKeyT> finalKeyClass;
  private final transient List<BucketedInput<?, ?>> sources;

  public SortedBucketSource(Class<FinalKeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    this.finalKeyClass = finalKeyClass;
    this.sources = sources;
  }

  @Override
  public final PCollection<KV<FinalKeyT, CoGbkResult>> expand(PBegin begin) {
    final SourceSpec<FinalKeyT> sourceSpec = getSourceSpec(finalKeyClass, sources);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<Integer> reshuffle = Reshuffle.viaRandomKey();

    final TupleTag<?>[] tupleTags = BucketedInput.collectTupleTags(sources);
    final CoGbkResultSchema resultSchema = CoGbkResultSchema.of(Arrays.asList(tupleTags));
    final CoGbkResult.CoGbkResultCoder resultCoder =
        CoGbkResult.CoGbkResultCoder.of(
            resultSchema,
            UnionCoder.of(
                sources.stream().map(BucketedInput::getCoder).collect(Collectors.toList())));

    return begin.getPipeline()
        .apply("CreateBuckets",
            Create.of(
                IntStream.range(0, sourceSpec.leastNumBuckets).boxed().collect(Collectors.toList())
            ).withCoder(VarIntCoder.of()))
        .apply("ReshuffleKeys", reshuffle)
        .apply("MergeBuckets", ParDo.of(new MergeBuckets<>(sources, sourceSpec)))
        .setCoder(KvCoder.of(sourceSpec.keyCoder, resultCoder));
  }

  static class SourceSpec<K> {
    int leastNumBuckets;
    Coder<K> keyCoder;

    SourceSpec(int leastNumBuckets, Coder<K> keyCoder) {
      this.leastNumBuckets = leastNumBuckets;
      this.keyCoder = keyCoder;
    }
  }

  static <KeyT> SourceSpec<KeyT> getSourceSpec(
      Class<KeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources
  ) {
    BucketMetadata<?, ?> first = null;
    Coder<KeyT> finalKeyCoder = null;

    int leastNumBuckets = Integer.MAX_VALUE;
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

      leastNumBuckets = Math.min(current.getNumBuckets(), leastNumBuckets);

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

    Preconditions.checkNotNull(
        finalKeyCoder, "Could not infer coder for key class %s", finalKeyClass);

    return new SourceSpec<>(leastNumBuckets, finalKeyCoder);
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBuckets<FinalKeyT> extends DoFn<Integer, KV<FinalKeyT, CoGbkResult>> {
    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final Integer leastNumBuckets;
    private final Coder<FinalKeyT> keyCoder;
    private final List<BucketedInput<?, ?>> sources;

    MergeBuckets(List<BucketedInput<?, ?>> sources, SourceSpec<FinalKeyT> sourceSpec) {
      this.leastNumBuckets = sourceSpec.leastNumBuckets;
      this.keyCoder = sourceSpec.keyCoder;
      this.sources = sources;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final int bucketId = c.element();
      final int numSources = sources.size();

      final KeyGroupIterator[] iterators = sources.stream()
          .map(i -> i.createIterator(bucketId, leastNumBuckets))
          .toArray(KeyGroupIterator[]::new);

      final TupleTag[] tupleTags = BucketedInput.collectTupleTags(sources);
      final CoGbkResultSchema resultSchema = CoGbkResultSchema.of(Arrays.asList(tupleTags));

      final Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        int completedSources = 0;
        // Advance key-value groups from each source
        for (int i = 0; i < numSources; i++) {
          final KeyGroupIterator it = iterators[i];
          if (nextKeyGroups.containsKey(tupleTags[i])) {
            continue;
          }
          if (it.hasNext()) {
            @SuppressWarnings("unchecked")
            final KV<byte[], Iterator<?>> next = it.next();
            nextKeyGroups.put(tupleTags[i], next);
          } else {
            completedSources++;
          }
        }

        if (nextKeyGroups.isEmpty()) {
          break;
        }

        // Find next key-value groups
        c.output(mergeKeyGroup(nextKeyGroups, resultSchema, keyCoder));

        if (completedSources == numSources) {
          break;
        }
      }
    }

    static <K> KV<K, CoGbkResult> mergeKeyGroup(
        Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups,
        CoGbkResultSchema resultSchema,
        Coder<K> keyCoder
    ) {
      final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry =
          nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);

      final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
          nextKeyGroups.entrySet().iterator();
      final List<Iterable<?>> valueMap = new ArrayList<>();
      for (int i = 0; i < resultSchema.size(); i++) {
        valueMap.add(new ArrayList<>());
      }

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
        }
      }

      // Output next key-value group
      final ByteArrayInputStream groupKeyBytes =
          new ByteArrayInputStream(minKeyEntry.getValue().getKey());
      final K groupKey;
      try {
        groupKey = keyCoder.decode(groupKeyBytes);
      } catch (Exception e) {
        throw new RuntimeException("Could not decode key bytes for group", e);
      }
      return KV.of(groupKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("keyCoder", keyCoder.getClass()));
      builder.add(DisplayData.item("leastNumBuckets", leastNumBuckets));
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

    private transient Map<ResourceId, DirectoryMetadata> inputMetadata;
    private transient BucketMetadata<K, V> canonicalMetadata;

    private static class DirectoryMetadata implements Serializable {
      final FileAssignment fileAssignment;
      final int numBuckets;
      final int numShards;

      DirectoryMetadata(FileAssignment fileAssignment, BucketMetadata<?, ?> bucketMetadata) {
        this.fileAssignment = fileAssignment;
        this.numBuckets = bucketMetadata.getNumBuckets();
        this.numShards = bucketMetadata.getNumShards();
      }
    }

    public BucketedInput(
        TupleTag<V> tupleTag,
        ResourceId inputDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations
    ) {
      this(tupleTag, Collections.singletonList(inputDirectory), filenameSuffix, fileOperations);
    }

    public BucketedInput(
        TupleTag<V> tupleTag,
        List<ResourceId> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations
    ) {
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

    static TupleTag<?>[] collectTupleTags(List<BucketedInput<?, ?>> sources) {
      return sources.stream()
          .map(BucketedInput::getTupleTag)
          .toArray(TupleTag[]::new);
    }

    public BucketMetadata<K, V> getMetadata() {
      computeMetadataIfAbsent();
      return canonicalMetadata;
    }

    private void computeMetadataIfAbsent() {
      if (this.inputMetadata != null && this.canonicalMetadata != null) {
        return;
      }

      this.inputMetadata = new HashMap<>();
      this.canonicalMetadata = null;
      ResourceId canonicalMetadataDir = null;

      for (ResourceId dir : inputDirectories) {
        final FileAssignment fileAssignment =
            new SMBFilenamePolicy(dir, filenameSuffix).forDestination();
        BucketMetadata<K, V> metadata;
        try {
          metadata = BucketMetadata.from(
              Channels.newInputStream(FileSystems.open(fileAssignment.forMetadata()))
          );
        } catch (IOException e) {
          throw new RuntimeException("Error fetching metadata for " + dir, e);
        }
        if (
            this.canonicalMetadata == null ||
                metadata.getNumBuckets() < this.canonicalMetadata.getNumBuckets()
        ) {
          this.canonicalMetadata = metadata;
          canonicalMetadataDir = dir;
        }

        Preconditions.checkState(
            metadata.isCompatibleWith(this.canonicalMetadata) &&
                metadata.isPartitionCompatible(this.canonicalMetadata),
            "%s cannot be read as a single input source. Metadata in directory "
                + "%s is incompatible with metadata in directory %s. %s != %s",
            this, dir, canonicalMetadataDir, metadata, canonicalMetadata);

        this.inputMetadata.put(
            dir,
            new DirectoryMetadata(fileAssignment, metadata)
        );
      }
    }

    KeyGroupIterator<byte[], V> createIterator(int bucketId, int leastNumBuckets) {
      final List<Iterator<V>> iterators = new ArrayList<>();

      // Create one iterator per shard
      inputMetadata.forEach((resourceId, directoryMetadata) -> {
        final int directoryBuckets = directoryMetadata.numBuckets;
        final int directoryShards = directoryMetadata.numShards;

        // Since all BucketedInputs have a bucket count that's a power of two, we can infer
        // which buckets should be merged together for the join.
        for (int i = bucketId; i < directoryBuckets; i += leastNumBuckets) {
          for (int j = 0; j < directoryShards; j++) {
            final ResourceId file = directoryMetadata
                .fileAssignment
                .forBucket(BucketShardId.of(i, j), directoryBuckets, directoryShards);
            try {
              iterators.add(fileOperations.iterator(file));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

      final Function<V, byte[]> keyBytesFn = canonicalMetadata::getKeyBytes;

      // Merge-sort key-values from shards
      final UnmodifiableIterator<V> iterator =
          Iterators.mergeSorted(
              iterators,
              (o1, o2) ->
                  bytesComparator.compare(
                      keyBytesFn.apply(o1), keyBytesFn.apply(o2)));
      return new KeyGroupIterator<>(iterator, keyBytesFn, bytesComparator);
    }

    @Override
    public String toString() {
      return String.format(
          "BucketedInput[tupleTag=%s, inputDirectories=[%s], metadata=%s]",
          tupleTag.getId(),
          inputDirectories.size() > 5 ?
              inputDirectories.subList(0, 4) + "..." +
                  inputDirectories.get(inputDirectories.size() - 1)
              : inputDirectories,
          canonicalMetadata);
    }

    // Not all instance members can be natively serialized, so override writeObject/readObject
    // using Coders for each type
    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream outStream) throws IOException {
      SerializableCoder.of(TupleTag.class).encode(tupleTag, outStream);
      StringUtf8Coder.of().encode(filenameSuffix, outStream);
      SerializableCoder.of(FileOperations.class).encode(fileOperations, outStream);

      // Depending on when .writeObject is called, metadata may not have been computed.
      NullableCoder.of(MapCoder.of(
          ResourceIdCoder.of(),
          SerializableCoder.of(DirectoryMetadata.class)
      )).encode(inputMetadata, outStream);

      NullableCoder.of(
          new BucketMetadataCoder<K, V>()
      ).encode(canonicalMetadata, outStream);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inStream) throws ClassNotFoundException, IOException {
      this.tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
      this.filenameSuffix = StringUtf8Coder.of().decode(inStream);
      this.fileOperations = SerializableCoder.of(FileOperations.class).decode(inStream);

      this.inputMetadata = NullableCoder.of(MapCoder.of(
          ResourceIdCoder.of(),
          SerializableCoder.of(DirectoryMetadata.class)
      )).decode(inStream);
      this.canonicalMetadata = NullableCoder.of(new BucketMetadataCoder<K, V>()).decode(inStream);
    }
  }
}
