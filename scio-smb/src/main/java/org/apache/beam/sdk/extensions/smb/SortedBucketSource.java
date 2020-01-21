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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
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
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
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
    BucketMetadata<?, ?> first = null;
    Coder<FinalKeyT> finalKeyCoder = null;

    int leastNumBuckets = Integer.MAX_VALUE;
    // Check metadata of each source
    for (BucketedInput<?, ?> source : sources) {
      source.validateSourcesCompatibility();
      final BucketMetadata<?, ?> current = source.getCanonicalMetadata();
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
          final Coder<FinalKeyT> coder = (Coder<FinalKeyT>) current.getKeyCoder();
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

    // Expand sources into one key-value pair per bucket
    @SuppressWarnings("unchecked")
    final PCollection<KV<Integer, List<BucketedInput<?, ?>>>> bucketedInputs =
        (PCollection<KV<Integer, List<BucketedInput<?, ?>>>>)
            new BucketedInputs(begin.getPipeline(), leastNumBuckets, sources)
                .expand()
                .get(new TupleTag<>("BucketedSources"));

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<KV<Integer, List<BucketedInput<?, ?>>>> reshuffle =
        Reshuffle.viaRandomKey();

    final List<TupleTag<?>> tupleTags =
        sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList());
    final CoGbkResultSchema resultSchema = CoGbkResultSchema.of(tupleTags);
    final CoGbkResult.CoGbkResultCoder resultCoder =
        CoGbkResult.CoGbkResultCoder.of(
            resultSchema,
            UnionCoder.of(
                sources.stream().map(BucketedInput::getCoder).collect(Collectors.toList())));

    return bucketedInputs
        .apply("ReshuffleKeys", reshuffle)
        .apply("MergeBuckets", ParDo.of(new MergeBuckets<>(leastNumBuckets, finalKeyCoder)))
        .setCoder(KvCoder.of(finalKeyCoder, resultCoder));
  }

  /** Merge key-value groups in matching buckets. */
  private static class MergeBuckets<FinalKeyT>
      extends DoFn<KV<Integer, List<BucketedInput<?, ?>>>, KV<FinalKeyT, CoGbkResult>> {

    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final Integer leastNumBuckets;
    private final Coder<FinalKeyT> keyCoder;

    MergeBuckets(int leastNumBuckets, Coder<FinalKeyT> keyCoder) {
      this.leastNumBuckets = leastNumBuckets;
      this.keyCoder = keyCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final int bucketId = c.element().getKey();
      final List<BucketedInput<?, ?>> sources = c.element().getValue();
      final int numSources = sources.size();

      // Initialize iterators and tuple tags for sources
      final KeyGroupIterator[] iterators =
          sources.stream()
              .map(i -> i.createIterator(bucketId, leastNumBuckets))
              .toArray(KeyGroupIterator[]::new);
      final List<TupleTag<?>> tupleTags =
          sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList());
      final CoGbkResultSchema resultSchema = CoGbkResultSchema.of(tupleTags);

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
        final FinalKeyT groupKey;
        try {
          groupKey = keyCoder.decode(groupKeyBytes);
        } catch (Exception e) {
          throw new RuntimeException("Could not decode key bytes for group", e);
        }
        c.output(KV.of(groupKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap)));

        if (completedSources == numSources) {
          break;
        }
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("keyCoder", keyCoder.getClass()));
      builder.add(DisplayData.item("leastNumBuckets", leastNumBuckets));
    }
  }

  /**
   * Abstracts a potentially heterogeneous list of sorted-bucket sources of {@link BucketedInput}s,
   * similar to {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}.
   */
  private static class BucketedInputs implements PInput {
    private final transient Pipeline pipeline;
    private final List<BucketedInput<?, ?>> sources;
    private final Integer numBuckets;

    private BucketedInputs(
        Pipeline pipeline, Integer numBuckets, List<BucketedInput<?, ?>> sources) {
      this.pipeline = pipeline;
      this.numBuckets = numBuckets;
      this.sources = sources;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      final List<KV<Integer, List<BucketedInput<?, ?>>>> sources = new ArrayList<>();
      for (int i = 0; i < numBuckets; i++) {
        try {
          sources.add(KV.of(i, this.sources));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @SuppressWarnings("unchecked")
      final KvCoder<Integer, List<BucketedInput<?, ?>>> coder =
          KvCoder.of(VarIntCoder.of(), ListCoder.of(new BucketedInput.BucketedInputCoder()));

      return Collections.singletonMap(
          new TupleTag<>("BucketedSources"),
          pipeline.apply("CreateBucketedSources", Create.of(sources).withCoder(coder)));
    }
  }

  /**
   * Abstracts a sorted-bucket input to {@link SortedBucketSource} written by {@link
   * SortedBucketSink}.
   *
   * @param <K> the type of the keys that values in a bucket are sorted with
   * @param <V> the type of the values in a bucket
   */
  public static class BucketedInput<K, V> {
    private final TupleTag<V> tupleTag;
    private final String filenameSuffix;
    private final FileOperations<V> fileOperations;
    private final List<ResourceId> inputDirectories;

    private transient Map<ResourceId, FileAssignment> fileAssignments;
    private transient Map<ResourceId, BucketMetadata<K, V>> metadata;
    private transient BucketMetadata<K, V> canonicalMetadata;

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
      this.inputDirectories = inputDirectories;
      this.filenameSuffix = filenameSuffix;
      this.fileOperations = fileOperations;

      this.fileAssignments = inputDirectories.stream().collect(
          Collectors.toMap(
            (ResourceId dir) -> dir,
            (ResourceId dir) -> new SMBFilenamePolicy(dir, filenameSuffix).forDestination()
          )
      ) ;
    }

    private BucketedInput(
        TupleTag<V> tupleTag,
        List<ResourceId> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Map<ResourceId, BucketMetadata<K, V>> metadata) {
      this(tupleTag, inputDirectories, filenameSuffix, fileOperations);
      this.metadata = metadata;
      getCanonicalMetadata();
    }

    public TupleTag<V> getTupleTag() {
      return tupleTag;
    }

    public BucketMetadata<K, V> getCanonicalMetadata() {
      if (canonicalMetadata == null) {
        canonicalMetadata = getMetadata().values().stream()
            .min(Comparator.comparingInt(BucketMetadata::getNumBuckets))
            .get();
      }

      return canonicalMetadata;
    }

    public Map<ResourceId, BucketMetadata<K, V>> getMetadata() {
      if (metadata == null) {
        metadata = fileAssignments.entrySet().stream().collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry -> {
                  try {
                    return BucketMetadata.from(
                        Channels.newInputStream(FileSystems.open(entry.getValue().forMetadata())));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }));
      }

      return metadata;
    }

    public Coder<V> getCoder() {
      return fileOperations.getCoder();
    }

    private void validateSourcesCompatibility() {
      if (getMetadata().size() == 1) {
        return;
      }

      Map.Entry<ResourceId, BucketMetadata<K, V>> first = null;
      for (Map.Entry<ResourceId, BucketMetadata<K, V>> current : getMetadata().entrySet()) {
        if (first == null) {
          first = current;
          continue;
        }

        Preconditions.checkState(
            first.getValue().isCompatibleWith(current.getValue()) &&
                first.getValue().isSameSourceCompatible(current.getValue()),
            "Metadata in directory %s is incompatible with metadata in directory %s: %s != %s",
            first.getKey(), current.getValue(), first.getValue(), current.getValue()
        );
      }
    }

    private KeyGroupIterator<byte[], V> createIterator(int bucketId, int leastNumBuckets) {
      final List<Iterator<V>> iterators = new ArrayList<>();

      // Create one iterator per shard
      fileAssignments.forEach((resourceId, fileAssignment) -> {
        final BucketMetadata<K, V> metadata = getMetadata().get(resourceId);

        final int numBucketsInSource = metadata.getNumBuckets();
        final int numShards = metadata.getNumShards();

        // Since all BucketedInputs have a bucket count that's a power of two, we can infer
        // which buckets should be merged together for the join.
        for (int i = bucketId; i < numBucketsInSource; i += leastNumBuckets) {
          for (int j = 0; j < numShards; j++) {
            final ResourceId file = fileAssignment.forBucket(BucketShardId.of(i, j), metadata);
            try {
              iterators.add(fileOperations.iterator(file));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

      final Function<V, byte[]> keyBytesFn = getCanonicalMetadata()::getKeyBytes;

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
          "BucketedInput[tupleTag=%s, inputDirectories=%s, metadata=%s]",
          tupleTag.getId(),
          inputDirectories.size() > 10 ?
              inputDirectories.subList(0, 8) + "..." + inputDirectories.get(inputDirectories.size() - 1)
              : inputDirectories,
          getCanonicalMetadata());
    }

    private static class BucketedInputCoder<K, V> extends AtomicCoder<BucketedInput<K, V>> {
      private static SerializableCoder<TupleTag> tupleTagCoder =
          SerializableCoder.of(TupleTag.class);
      private static ListCoder<ResourceId> inputDirectoriesCoder =
          ListCoder.of(ResourceIdCoder.of());
      private static StringUtf8Coder stringCoder = StringUtf8Coder.of();
      private static SerializableCoder<FileOperations> fileOpCoder =
          SerializableCoder.of(FileOperations.class);
      private MapCoder<ResourceId, BucketMetadata<K, V>> metadataMapCoder = MapCoder.of(
          ResourceIdCoder.of(),
          new BucketMetadataCoder<>()
      );



      @Override
      public void encode(BucketedInput<K, V> value, OutputStream outStream) throws IOException {
        tupleTagCoder.encode(value.tupleTag, outStream);
        inputDirectoriesCoder.encode(value.inputDirectories, outStream);
        stringCoder.encode(value.filenameSuffix, outStream);
        fileOpCoder.encode(value.fileOperations, outStream);
        metadataMapCoder.encode(value.getMetadata(), outStream);
      }

      @Override
      public BucketedInput<K, V> decode(InputStream inStream) throws IOException {
        @SuppressWarnings("unchecked")
        TupleTag<V> tupleTag = (TupleTag<V>) tupleTagCoder.decode(inStream);
        List<ResourceId> inputDirectories = inputDirectoriesCoder.decode(inStream);
        String filenameSuffix = stringCoder.decode(inStream);
        @SuppressWarnings("unchecked")
        FileOperations<V> fileOperations = fileOpCoder.decode(inStream);
        Map<ResourceId, BucketMetadata<K, V>> metadata = metadataMapCoder.decode(inStream);
        return new BucketedInput<>(
            tupleTag, inputDirectories, filenameSuffix, fileOperations, metadata);
      }
    }
  }
}
