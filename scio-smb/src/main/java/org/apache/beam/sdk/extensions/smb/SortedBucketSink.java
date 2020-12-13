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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.spotify.scio.transforms.DoFnWithResource;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for writing a {@link PCollection} to file-based sink, where files represent
 * "buckets" of elements deterministically assigned by {@link BucketMetadata} based on a key
 * extraction function. The elements in each bucket are written in sorted order according to the
 * same key.
 *
 * <p>This transform is intended to be used in conjunction with the {@link SortedBucketSource}
 * transform. Any two datasets written with {@link SortedBucketSink} using the same bucketing scheme
 * can be joined by simply sequentially reading and merging files, thus eliminating the shuffle
 * required by {@link GroupByKey}-based transforms. This is ideal for datasets that will be written
 * once and read many times with a predictable join key, i.e. user event data.
 *
 * <h3>Transform steps</h3>
 *
 * <p>{@link SortedBucketSink} re-uses existing {@link PTransform}s to map over each element,
 * extract a {@code byte[]} representation of its sorting key using {@link
 * BucketMetadata#extractKey(Object)} (Object)}, and assign it to an Integer bucket using {@link
 * BucketMetadata#getBucketId(byte[])}. Next, a {@link GroupByKey} transform is applied to create a
 * {@link PCollection} of {@code N} elements, where {@code N} is the number of buckets specified by
 * {@link BucketMetadata#getNumBuckets()}, then a {@code SortValues} transform is used to sort
 * elements within each bucket group. Finally, the write operation is performed, where each bucket
 * is first written to a {@link SortedBucketSink#tempDirectory} and then copied to its final
 * destination.
 *
 * <p>A JSON-serialized form of {@link BucketMetadata} is also written, which is required in order
 * to join {@link SortedBucketSink}s using the {@link SortedBucketSource} transform.
 *
 * <h3>Bucketing properties and hot keys</h3>
 *
 * <p>Bucketing properties are specified in {@link BucketMetadata}. The number of buckets, {@code
 * N}, must be a power of two and should be chosen such that each bucket can fit in a worker node's
 * memory. Note that the {@code SortValues} transform will try to sort in-memory and fall back to an
 * {@link ExternalSorter} if needed.
 *
 * <p>Each bucket can be further sharded to reduce the impact of hot keys, by specifying {@link
 * BucketMetadata#getNumShards()}.
 *
 * @param <K> the type of the keys that values in a bucket are sorted with
 * @param <V> the type of the values in a bucket
 */
public class SortedBucketSink<K, V, T> extends PTransform<PCollection<V>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSink.class);

  private final BucketMetadata<K, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<T> fileOperations;
  private final int sorterMemoryMb;
  private final int keyCacheSize;
  private final SerializableBiFunction<K, Iterable<V>, Iterable<T>> groupMappingFn;
  private Coder<T> outputValueCoder;

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<T> fileOperations,
      int sorterMemoryMb) {
    this(
        bucketMetadata,
        outputDirectory,
        tempDirectory,
        filenameSuffix,
        fileOperations,
        sorterMemoryMb,
        0);
  }

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<T> fileOperations,
      int sorterMemoryMb,
      int keyCacheSize) {
    this(
        bucketMetadata,
        outputDirectory,
        tempDirectory,
        filenameSuffix,
        fileOperations,
        sorterMemoryMb,
        keyCacheSize,
        null,
        null);
  }

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<T> fileOperations,
      int sorterMemoryMb,
      int keyCacheSize,
      SerializableBiFunction<K, Iterable<V>, Iterable<T>> groupMappingFn,
      Coder<T> outputValueCoder) {
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy =
        new SMBFilenamePolicy(outputDirectory, bucketMetadata.getFilenamePrefix(), filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.sorterMemoryMb = sorterMemoryMb;
    this.keyCacheSize = keyCacheSize;
    this.groupMappingFn = groupMappingFn;
    this.outputValueCoder = outputValueCoder;

//    Preconditions.checkArgument(
//         (groupMappingFn != null && outputValueCoder != null) || (groupMappingFn == null && outputValueCoder != null),
//         "Group mapping function's output value coder is not set");
  }

  @Override
  public final WriteResult expand(PCollection<V> input) {
    // @Todo: should we allow windowed writes?
    Preconditions.checkArgument(
        input.isBounded() == IsBounded.BOUNDED,
        "SortedBucketSink cannot be applied to a non-bounded PCollection");
    final Coder<V> inputValueCoder = input.getCoder();

    final PCollection<KV<BucketShardId, KV<byte[], V>>> bucketedInput =
        input.apply(
            "ExtractKeys",
            ParDo.of(
                ExtractKeys.of(
                    bucketMetadata, bucketMetadata::extractKey, v -> v, keyCacheSize)));

    return sink(
        bucketedInput,
        getName(),
        inputValueCoder,
        outputValueCoder,
        sorterMemoryMb,
        filenamePolicy,
        fileOperations,
        bucketMetadata,
        tempDirectory,
        groupMappingFn);
  }

  public static <KeyT, ValueV, ValueT> WriteResult sink(
      PCollection<KV<BucketShardId, KV<byte[], ValueV>>> bucketedInput,
      String transformName,
      Coder<ValueV> inputValueCoder,
      Coder<ValueT> outputValueCoder,
      int sorterMemoryMb,
      SMBFilenamePolicy filenamePolicy,
      FileOperations<ValueT> fileOperations,
      BucketMetadata<KeyT, ValueV> bucketMetadata,
      ResourceId tempDirectory,
      SerializableBiFunction<KeyT, Iterable<ValueV>, Iterable<ValueT>> groupMappingFn) {

    Coder<ValueT> effectiveOutputValueCoder = outputValueCoder;
    // if group mapping function is not present then type V and type T must be the same.
    // Hence following is a safe cast.
    if(groupMappingFn == null) {
      effectiveOutputValueCoder = (Coder<ValueT>) inputValueCoder;
    }

    return bucketedInput
        .setCoder(
            KvCoder.of(
                BucketShardIdCoder.of(), KvCoder.of(ByteArrayCoder.of(), inputValueCoder)))
        .apply("GroupByKey", GroupByKey.create())
        .apply("Group Mapping",
            ParDo.of(
                new GroupMappingDoFn<>(
                    groupMappingFn,
                    inputValueCoder,
                    outputValueCoder,
                    bucketMetadata)))
        .apply(
            "SortValues",
            ParDo.of(
                new SortBytesDoFn<>(
                    transformName,
                    BufferedExternalSorter.options()
                        .withExternalSorterType(ExternalSorter.Options.SorterType.NATIVE)
                        .withMemoryMB(sorterMemoryMb))))
        .apply(
            "WriteOperation",
            new WriteOperation<>(
                filenamePolicy, bucketMetadata, fileOperations, tempDirectory, effectiveOutputValueCoder));
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  private static class ExtractKeys<K, U, V> extends DoFn<U, KV<BucketShardId, KV<byte[], V>>> {
    // Substitute null keys in the output KV<byte[], V> so that they survive serialization
    static final byte[] NULL_SORT_KEY = new byte[0];
    private final SerializableFunction<U, K> extractKeyFn;
    private final SerializableFunction<U, V> extractValueFn;
    private final BucketMetadata<K, ?> bucketMetadata;
    private transient int shardId;

    static <KeyT, ValueU, ValueV> DoFn<ValueU, KV<BucketShardId, KV<byte[], ValueV>>> of(
        BucketMetadata<KeyT, ?> bucketMetadata,
        SerializableFunction<ValueU, KeyT> extractKeyFn,
        SerializableFunction<ValueU, ValueV> extractValueFn,
        int keyCacheSize) {
      if (keyCacheSize == 0) {
        return new ExtractKeys<>(bucketMetadata, extractKeyFn, extractValueFn);
      } else {
        return new ExtractKeysWithCache<>(bucketMetadata, extractKeyFn, extractValueFn, keyCacheSize);
      }
    }

    private ExtractKeys(
        BucketMetadata<K, ?> bucketMetadata,
        SerializableFunction<U, K> extractKeyFn,
        SerializableFunction<U, V> extractValueFn) {
      this.bucketMetadata = bucketMetadata;
      this.extractKeyFn = extractKeyFn;
      this.extractValueFn = extractValueFn;
    }

    // From Combine.PerKeyWithHotKeyFanout.
    @StartBundle
    public void startBundle() {
      // Spreading a hot key across all possible sub-keys for all bundles
      // would defeat the goal of not overwhelming downstream reducers
      // (as well as making less efficient use of PGBK combining tables).
      // Instead, each bundle independently makes a consistent choice about
      // which "shard" of a key to send its intermediate results.
      shardId =
          ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) % bucketMetadata.getNumShards();
    }

    static <KeyT> KV<BucketShardId, byte[]> processKey(
        KeyT key, BucketMetadata<KeyT, ?> metadata, int shardId) {
      final byte[] keyBytes = metadata.encodeKeyBytes(key);

      return (keyBytes != null)
          ? KV.of(BucketShardId.of(metadata.getBucketId(keyBytes), shardId), keyBytes)
          : KV.of(BucketShardId.ofNullKey(), NULL_SORT_KEY);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final U record = c.element();
      final KV<BucketShardId, byte[]> bucketAndSortKey =
          processKey(extractKeyFn.apply(record), bucketMetadata, shardId);

      c.output(
          KV.of(
              bucketAndSortKey.getKey(),
              KV.of(bucketAndSortKey.getValue(), extractValueFn.apply(record))));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
    }
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  private static class ExtractKeysWithCache<K, U, V>
      extends DoFnWithResource<
          U, KV<BucketShardId, KV<byte[], V>>, Cache<K, KV<Integer, byte[]>>> {
    // Substitute null keys in the output KV<byte[], V> so that they survive serialization
    private final SerializableFunction<U, K> extractKeyFn;
    private final SerializableFunction<U, V> extractValueFn;
    private final BucketMetadata<K, ?> bucketMetadata;
    private final int cacheSize;
    private transient int shardId;
    private Counter cacheHits;
    private Counter cacheMisses;

    ExtractKeysWithCache(
        BucketMetadata<K, ?> bucketMetadata,
        SerializableFunction<U, K> extractKeyFn,
        SerializableFunction<U, V> extractValueFn,
        int cacheSize) {
      this.bucketMetadata = bucketMetadata;
      this.extractKeyFn = extractKeyFn;
      this.extractValueFn = extractValueFn;
      this.cacheSize = cacheSize;
      cacheHits = Metrics.counter(SortedBucketSink.class, "cacheHits");
      cacheMisses = Metrics.counter(SortedBucketSink.class, "cacheMisses");
    }

    @Override
    public ResourceType getResourceType() {
      return ResourceType.PER_INSTANCE;
    }

    @Override
    public Cache<K, KV<Integer, byte[]>> createResource() {
      return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    @StartBundle
    public void startBundle() {
      shardId =
          ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) % bucketMetadata.getNumShards();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final U record = c.element();
      final K key = extractKeyFn.apply(record);

      KV<BucketShardId, byte[]> bucketIdAndSortBytes;
      if (key == null) {
        bucketIdAndSortBytes = KV.of(BucketShardId.ofNullKey(), ExtractKeys.NULL_SORT_KEY);
      } else {
        bucketIdAndSortBytes =
            Optional.ofNullable(getResource().getIfPresent(key))
                .map(
                    kv -> {
                      cacheHits.inc();
                      return KV.of(BucketShardId.of(kv.getKey(), shardId), kv.getValue());
                    })
                .orElseGet(
                    () -> {
                      cacheMisses.inc();
                      final KV<BucketShardId, byte[]> kv =
                          ExtractKeys.processKey(key, bucketMetadata, shardId);
                      getResource().put(key, KV.of(kv.getKey().getBucketId(), kv.getValue()));
                      return kv;
                    });
      }

      c.output(
          KV.of(
              bucketIdAndSortBytes.getKey(),
              KV.of(
                  bucketIdAndSortBytes.getValue(), extractValueFn.apply(record))));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
      builder.add(DisplayData.item("keyCacheSize", cacheSize));
    }
  }

  /**
   * Patch of {@code SortValues}'s SortValuesDoFn which acts on elements already encoded as bytes
   * and can avoid the additional ser/de operation per element.
   */
  private static class SortBytesDoFn<K>
      extends DoFn<KV<K, Iterable<KV<byte[], byte[]>>>, KV<K, Iterable<KV<byte[], byte[]>>>> {
    private final BufferedExternalSorter.Options sorterOptions;
    private final Counter bucketsInitiatedSorting;
    private final Counter bucketsCompletedSorting;

    SortBytesDoFn(String transformName, BufferedExternalSorter.Options sorterOptions) {
      this.sorterOptions = sorterOptions;
      this.bucketsInitiatedSorting =
          Metrics.counter(SortedBucketSink.class, transformName + "-bucketsInitiatedSorting");
      this.bucketsCompletedSorting =
          Metrics.counter(SortedBucketSink.class, transformName + "-bucketsCompletedSorting");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final KV<K, Iterable<KV<byte[], byte[]>>> record = c.element();
      final BufferedExternalSorter sorter = BufferedExternalSorter.create(sorterOptions);

      try {
        bucketsInitiatedSorting.inc();

        for (KV<byte[], byte[]> kv : record.getValue()) {
          sorter.add(kv);
        }
        c.output(KV.of(record.getKey(), sorter.sort()));

        bucketsCompletedSorting.inc();
      } catch (IOException e) {
        throw new RuntimeException("Exception sorting buckets", e);
      }
    }
  }

  public static class GroupMappingDoFn<K, ValueU, ValueT> extends
          DoFn<KV<BucketShardId, Iterable<KV<byte[],ValueU>>>, KV<BucketShardId, Iterable<KV<byte[], byte[]>>>> {
    private final BiFunction<K, Iterable<ValueU>, Iterable<ValueT>> groupMappingFn;
    private final Coder<ValueU> inputValueCoder;
    private final Coder<ValueT> outputValueCoder;
    private final BucketMetadata<K, ValueU> bucketMetadata;

    public GroupMappingDoFn(
            BiFunction<K, Iterable<ValueU>, Iterable<ValueT>> groupMappingFn,
            Coder<ValueU> inputValueCoder,
            Coder<ValueT> outputValueCoder,
            BucketMetadata<K, ValueU> bucketMetadata) {
      this.groupMappingFn = groupMappingFn;
      this.inputValueCoder = inputValueCoder;
      this.outputValueCoder = outputValueCoder;
      this.bucketMetadata = bucketMetadata;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      KV<BucketShardId, Iterable<KV<byte[], ValueU>>> input = c.element();
      List<KV<byte[], byte[]>> output = new ArrayList<>();

      if (groupMappingFn == null) {
        input.getValue().forEach(kv -> output.add(KV.of(kv.getKey(), getValueBytes(inputValueCoder, kv.getValue()))));
      } else {
        ListMultimap<K, ValueU> multimap = MultimapBuilder.hashKeys().arrayListValues().build();
        for (KV<byte[], ValueU> kv : input.getValue()) {
          multimap.put(bucketMetadata.decodeKeyFromBytes(kv.getKey()), kv.getValue());
        }

        for (K key : multimap.keySet()) {
          Collection<ValueU> values = multimap.get(key);
          Iterable<ValueT> outputValues = groupMappingFn.apply(key, values);
          byte[] keyBytes = bucketMetadata.encodeKeyBytes(key);
          for (ValueT outputValue : outputValues) {
            output.add(KV.of(keyBytes, getValueBytes(outputValueCoder, outputValue)));
          }
        }
      }

      c.output(KV.of(input.getKey(), output));
    }
  }
  /**
   * The result of a successfully completed {@link SortedBucketSink} transform. Holds {@link
   * TupleTag} references to both the successfully written {@link BucketMetadata}, and to all
   * successfully written {@link BucketShardId}s.
   */
  public static class WriteResult implements POutput {
    private final Pipeline pipeline;
    private final PCollection<ResourceId> writtenMetadata;
    private final PCollection<KV<BucketShardId, ResourceId>> writtenFiles;

    WriteResult(
        Pipeline pipeline,
        PCollection<ResourceId> writtenMetadata,
        PCollection<KV<BucketShardId, ResourceId>> writtenFiles) {
      this.pipeline = pipeline;
      this.writtenMetadata = writtenMetadata;
      this.writtenFiles = writtenFiles;
    }

    static WriteResult fromTuple(PCollectionTuple tuple) {
      return new WriteResult(
          tuple.getPipeline(),
          tuple.get(new TupleTag<ResourceId>("writtenMetadata")).setCoder(ResourceIdCoder.of()),
          tuple
              .get(new TupleTag<KV<BucketShardId, ResourceId>>("writtenBuckets"))
              .setCoder(KvCoder.of(BucketShardIdCoder.of(), ResourceIdCoder.of())));
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          new TupleTag<>("WrittenMetadata"), writtenMetadata,
          new TupleTag<>("WrittenFiles"), writtenFiles);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  /**
   * Handles writing bucket data and SMB metadata to a uniquely named temp directory. Abstract
   * operation that manages the process of writing to {@link SortedBucketSink}.
   */
  static class WriteOperation<V>
      extends PTransform<
          PCollection<KV<BucketShardId, Iterable<KV<byte[], byte[]>>>>, WriteResult> {
    private final SMBFilenamePolicy filenamePolicy;
    private final BucketMetadata<?, ?> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final ResourceId tempDirectory;
    private final Coder<V> valueCoder;

    WriteOperation(
        SMBFilenamePolicy filenamePolicy,
        BucketMetadata<?, ?> bucketMetadata,
        FileOperations<V> fileOperations,
        ResourceId tempDirectory,
        Coder<V> valueCoder) {
      this.filenamePolicy = filenamePolicy;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
      this.tempDirectory = tempDirectory;
      this.valueCoder = valueCoder;
    }

    @Override
    public WriteResult expand(PCollection<KV<BucketShardId, Iterable<KV<byte[], byte[]>>>> input) {
      return WriteResult.fromTuple(
          input
              .apply(
                  "WriteTempFiles",
                  ParDo.of(
                      new WriteTempFiles<>(
                          filenamePolicy.forTempFiles(tempDirectory),
                          bucketMetadata,
                          fileOperations,
                          valueCoder)))
              .apply(
                  "FinalizeTempFiles",
                  new RenameBuckets<>(
                      filenamePolicy.forDestination(), bucketMetadata, fileOperations)));
    }
  }

  /** Writes metadata and bucket files to temporary location. */
  static class WriteTempFiles<V>
      extends DoFn<KV<BucketShardId, Iterable<KV<byte[], byte[]>>>, KV<BucketShardId, ResourceId>> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata<?,?> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final Coder<V> valueCoder;

    WriteTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata<?, ?> bucketMetadata,
        FileOperations<V> fileOperations,
        Coder<V> valueCoder) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
      this.valueCoder = valueCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      final BucketShardId bucketShardId = c.element().getKey();
      final Iterable<KV<byte[], byte[]>> records = c.element().getValue();
      final ResourceId tmpFile = fileAssignment.forBucket(bucketShardId, bucketMetadata);

      LOG.info("Writing sorted-bucket {} to temporary file {}", bucketShardId, tmpFile);
      try (final FileOperations.Writer<V> writer = fileOperations.createWriter(tmpFile)) {
        records.forEach(
            kv -> {
              try {
                writer.write(CoderUtils.decodeFromByteArray(valueCoder, kv.getValue()));
              } catch (IOException e) {
                cleanupTempFiles(e, Collections.singleton(tmpFile));
                throw new RuntimeException(
                    "Failed to write sorted-bucket file " + bucketShardId, e);
              }
            });
      }

      c.output(KV.of(bucketShardId, tmpFile));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(fileAssignment);
      builder.delegate(fileOperations);
    }
  }

  /** Renames temp bucket files to final destinations. */
  static class RenameBuckets<V>
      extends PTransform<PCollection<KV<BucketShardId, ResourceId>>, PCollectionTuple> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;

    RenameBuckets(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        FileOperations<V> fileOperations) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<BucketShardId, ResourceId>> input) {
      final PCollectionView<Map<BucketShardId, ResourceId>> writtenBuckets =
          input.apply("WrittenBucketShardIds", View.asMap());

      final TupleTag<KV<BucketShardId, ResourceId>> bucketsTag = new TupleTag<>("writtenBuckets");
      final TupleTag<ResourceId> metadataTag = new TupleTag<>("writtenMetadata");

      return input
          .getPipeline()
          .apply("InitializeTmpMove", Create.of(0))
          .apply(
              "PopulateFinalDst",
              ParDo.of(
                      new DoFn<Integer, KV<BucketShardId, ResourceId>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          moveFiles(
                              bucketMetadata,
                              c.sideInput(writtenBuckets),
                              fileAssignment,
                              fileOperations,
                              bucketDst -> c.output(bucketsTag, bucketDst),
                              metadataDst -> c.output(metadataTag, metadataDst),
                              true);
                        }
                      })
                  .withSideInputs(writtenBuckets)
                  .withOutputTags(bucketsTag, TupleTagList.of(metadataTag)));
    }

    static void moveFiles(
        BucketMetadata<?, ?> bucketMetadata,
        Map<BucketShardId, ResourceId> writtenTmpBuckets,
        FileAssignment dstFileAssignment,
        FileOperations fileOperations,
        Consumer<KV<BucketShardId, ResourceId>> bucketDstConsumer,
        Consumer<ResourceId> metadataDstConsumer,
        boolean writeNullKeyBucket) {
      // Write metadata file first
      final ResourceId metadataDst =
          writeMetadataFile(dstFileAssignment.forMetadata(), bucketMetadata);

      final List<ResourceId> filesToCleanUp = new ArrayList<>();
      filesToCleanUp.add(metadataDst);

      // Transfer bucket files once metadata has been written
      final List<ResourceId> srcFiles = new ArrayList<>();
      final List<ResourceId> dstFiles = new ArrayList<>();
      final List<KV<BucketShardId, ResourceId>> bucketDsts = new ArrayList<>();

      final Set<BucketShardId> allBucketShardIds = bucketMetadata.getAllBucketShardIds();
      if (writeNullKeyBucket) {
        allBucketShardIds.add(BucketShardId.ofNullKey());
      }

      for (BucketShardId id : allBucketShardIds) {
        final ResourceId finalDst = dstFileAssignment.forBucket(id, bucketMetadata);

        // If bucket hasn't been written, write empty file
        if (!writtenTmpBuckets.containsKey(id)) {
          try {
            fileOperations.createWriter(finalDst).close();
            filesToCleanUp.add(finalDst);
          } catch (IOException e) {
            cleanupTempFiles(e, filesToCleanUp);
            throw new RuntimeException("Failed to write empty file", e);
          }

          bucketDstConsumer.accept(KV.of(id, finalDst));
        } else {
          srcFiles.add(writtenTmpBuckets.get(id));
          dstFiles.add(finalDst);
          bucketDsts.add(KV.of(id, finalDst));
        }
      }

      LOG.info("Moving {} bucket files into {}", srcFiles.size(), dstFileAssignment.getDirectory());
      try {
        FileSystems.rename(srcFiles, dstFiles);
      } catch (IOException e) {
        cleanupTempFiles(e, filesToCleanUp);
        throw new RuntimeException("Failed to rename temporary files", e);
      }

      bucketDsts.forEach(bucketDstConsumer::accept);
      metadataDstConsumer.accept(metadataDst);
    }

    @SuppressWarnings("unchecked")
    static ResourceId writeMetadataFile(ResourceId dst, BucketMetadata metadata) {
      LOG.info("Writing metadata to file {}", dst);

      try (final OutputStream outputStream =
          Channels.newOutputStream(FileSystems.create(dst, "application/json"))) {
        BucketMetadata.to(metadata, outputStream);
        return dst;
      } catch (IOException e) {
        cleanupTempFiles(e, Collections.singleton(dst));
        throw new RuntimeException("Failed to write metadata file", e);
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(fileAssignment);
    }
  }

  static void cleanupTempFiles(Exception cause, Collection<ResourceId> files) {
    LOG.info(
        "Deleting temporary file {}",
        files.stream().map(ResourceId::toString).collect(Collectors.joining(", ")));
    try {
      FileSystems.delete(files, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    } catch (IOException e) {
      cause.addSuppressed(e);
    }
  }

  static <ValueT> byte[] getValueBytes(Coder<ValueT> coder, ValueT value) {
    try {
      return CoderUtils.encodeToByteArray(coder, value);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  public static class SortedBucketPreKeyedSink<K, V, T>
      extends PTransform<PCollection<KV<K, V>>, WriteResult> {

    private final BucketMetadata<K, V> bucketMetadata;
    private final SMBFilenamePolicy filenamePolicy;
    private final ResourceId tempDirectory;
    private final FileOperations<T> fileOperations;
    private final int sorterMemoryMb;
    private final int keyCacheSize;
    private final Coder<V> inputValueCoder;
    private final boolean verifyKeyExtraction;
    private final SerializableBiFunction<K, Iterable<V>, Iterable<T>> groupMappingFn;
    private final Coder<T> outputValueCoder;

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<T> fileOperations,
        int sorterMemoryMb,
        Coder<V> inputValueCoder) {
      this(
          bucketMetadata,
          outputDirectory,
          tempDirectory,
          filenameSuffix,
          fileOperations,
          sorterMemoryMb,
          inputValueCoder,
          true);
    }

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<T> fileOperations,
        int sorterMemoryMb,
        Coder<V> inputValueCoder,
        boolean verifyKeyExtraction) {
      this(
          bucketMetadata,
          outputDirectory,
          tempDirectory,
          filenameSuffix,
          fileOperations,
          sorterMemoryMb,
              inputValueCoder,
          verifyKeyExtraction,
          0,
          null,
          null);
    }

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<T> fileOperations,
        int sorterMemoryMb,
        Coder<V> inputValueCoder,
        boolean verifyKeyExtraction,
        int keyCacheSize,
        SerializableBiFunction<K, Iterable<V>, Iterable<T>> groupMappingFn,
        Coder<T> outputValueCoder) {
      this.bucketMetadata = bucketMetadata;
      this.filenamePolicy =
          new SMBFilenamePolicy(
              outputDirectory, bucketMetadata.getFilenamePrefix(), filenameSuffix);
      this.tempDirectory = tempDirectory;
      this.fileOperations = fileOperations;
      this.sorterMemoryMb = sorterMemoryMb;
      this.keyCacheSize = keyCacheSize;
      this.verifyKeyExtraction = verifyKeyExtraction;
      this.inputValueCoder = inputValueCoder;
      this.groupMappingFn = groupMappingFn;
      this.outputValueCoder = outputValueCoder;
    }

    @Override
    public final WriteResult expand(PCollection<KV<K, V>> input) {
      // @Todo: should we allow windowed writes?
      Preconditions.checkArgument(
          input.isBounded() == IsBounded.BOUNDED,
          "SortedBucketSink cannot be applied to a non-bounded PCollection");

      final PCollection<KV<BucketShardId, KV<byte[], V>>> bucketedInput =
          input.apply(
              "ExtractKeys",
              ParDo.of(
                  ExtractKeys.of(
                      bucketMetadata,
                      KV::getKey,
                      KV::getValue,
                      keyCacheSize)));

      if (verifyKeyExtraction) {
        input
            .apply("VerifySample", Sample.any(100))
            .apply(
                "VerifyKeyFn",
                ParDo.of(
                    new DoFn<KV<K, V>, Void>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        final K key = bucketMetadata.extractKey(c.element().getValue());
                        final Coder<K> kCoder = NullableCoder.of(bucketMetadata.getKeyCoder());
                        if (!Arrays.equals(
                            CoderUtils.encodeToByteArray(kCoder, key),
                            CoderUtils.encodeToByteArray(kCoder, c.element().getKey()))) {
                          throw new RuntimeException(
                              "BucketMetadata's extractKey fn did not match pre-keyed PCollection");
                        }
                      }
                    }));
      }

      return SortedBucketSink.sink(
          bucketedInput,
          getName(),
          inputValueCoder,
          outputValueCoder,
          sorterMemoryMb,
          filenamePolicy,
          fileOperations,
          bucketMetadata,
          tempDirectory,
          groupMappingFn);
    }
  }
}
