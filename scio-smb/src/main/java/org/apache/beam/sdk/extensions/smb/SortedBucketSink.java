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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
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
 * <p>{@link SortedBucketSink} maps over each element, extracts a {@code byte[]} representation of
 * its sorting key using {@link BucketMetadata#extractKeyPrimary(Object)}, and assigns it to an
 * Integer bucket using {@link BucketMetadata#getBucketId(byte[])}. Next, a {@link GroupByKey}
 * transform is applied to create a {@link PCollection} of {@code N} elements, where {@code N} is
 * the number of buckets specified by {@link BucketMetadata#getNumBuckets()}, then a {@code
 * SortBucketShard} transform is used to sort elements within each bucket group, optionally sorting
 * by the secondary key bytes from {@link BucketMetadata#getKeyBytesSecondary(Object)}. Finally, the
 * write operation is performed, where each bucket is first written to a {@link
 * SortedBucketSink#tempDirectory} and then copied to its final destination.
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
 * @param <K1> the type of the primary keys that values in a bucket are sorted with
 * @param <K2> the type of the secondary keys that values in a bucket are sorted with, Void if not
 *     secondary sorted
 * @param <V> the type of the values in a bucket
 */
public class SortedBucketSink<K1, K2, V> extends PTransform<PCollection<V>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSink.class);

  private final BucketMetadata<K1, K2, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<V> fileOperations;
  private final int sorterMemoryMb;
  private final int keyCacheSize;

  public SortedBucketSink(
      BucketMetadata<K1, K2, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
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
      BucketMetadata<K1, K2, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
      int sorterMemoryMb,
      int keyCacheSize) {
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy =
        new SMBFilenamePolicy(outputDirectory, bucketMetadata.getFilenamePrefix(), filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.sorterMemoryMb = sorterMemoryMb;
    this.keyCacheSize = keyCacheSize;
  }

  @Override
  public final WriteResult expand(PCollection<V> input) {
    // @Todo: should we allow windowed writes?
    Preconditions.checkArgument(
        input.isBounded() == IsBounded.BOUNDED,
        "SortedBucketSink cannot be applied to a non-bounded PCollection");
    final Coder<V> inputCoder = input.getCoder();

    final PCollection<KV<BucketShardId, V>> bucketedInput =
        input.apply(
            "ExtractBucketAndShards",
            ParDo.of(ExtractBucketAndShardDoFn.of(bucketMetadata, keyCacheSize)));

    return sink(
        bucketedInput,
        getName(),
        inputCoder,
        sorterMemoryMb,
        filenamePolicy,
        fileOperations,
        bucketMetadata,
        tempDirectory);
  }

  public static <K1, K2, V> WriteResult sink(
      PCollection<KV<BucketShardId, V>> bucketedInput,
      String transformName,
      Coder<V> valueCoder,
      int sorterMemoryMb,
      SMBFilenamePolicy filenamePolicy,
      FileOperations<V> fileOperations,
      BucketMetadata<K1, K2, V> bucketMetadata,
      ResourceId tempDirectory) {
    return bucketedInput
        .setCoder(KvCoder.of(BucketShardIdCoder.of(), valueCoder))
        .apply("GroupByKey", GroupByKey.create())
        .apply(
            "SortBucketShards",
            ParDo.of(
                new SortBucketShardDoFn<>(
                    transformName,
                    BufferedExternalSorter.options()
                        .withExternalSorterType(ExternalSorter.Options.SorterType.NATIVE)
                        .withMemoryMB(sorterMemoryMb),
                    bucketMetadata,
                    valueCoder)))
        .apply(
            "WriteOperation",
            new WriteOperation<>(
                filenamePolicy, bucketMetadata, fileOperations, tempDirectory, valueCoder));
  }

  /** Extract bucket and shard id for grouping */
  private static class ExtractBucketAndShardDoFn<K1, V, InputT>
      extends DoFn<InputT, KV<BucketShardId, V>> {
    // Substitute null keys in the output KV<byte[], V> so that they survive serialization
    static final byte[] NULL_SORT_KEY = new byte[0];
    private final BucketMetadata<K1, ?, V> bucketMetadata;
    private final SerializableFunction<InputT, K1> primaryKeyFn;
    private final SerializableFunction<InputT, V> valueFn;
    private transient int shardId;

    static <K1, V> DoFn<V, KV<BucketShardId, V>> of(
        BucketMetadata<K1, ?, V> bucketMetadata, int keyCacheSize) {
      if (keyCacheSize == 0) {
        return new ExtractBucketAndShardDoFn<>(
            bucketMetadata, bucketMetadata::extractKeyPrimary, v -> v);
      } else {
        return new ExtractKeysWithCache<>(
            bucketMetadata, bucketMetadata::extractKeyPrimary, v -> v, keyCacheSize);
      }
    }

    static <K1, V> DoFn<KV<K1, V>, KV<BucketShardId, V>> preKeyed(
        BucketMetadata<K1, ?, V> bucketMetadata, int keyCacheSize) {
      if (keyCacheSize == 0) {
        return new ExtractBucketAndShardDoFn<>(bucketMetadata, KV::getKey, KV::getValue);
      } else {
        return new ExtractKeysWithCache<>(bucketMetadata, KV::getKey, KV::getValue, keyCacheSize);
      }
    }

    private ExtractBucketAndShardDoFn(
        BucketMetadata<K1, ?, V> bucketMetadata,
        SerializableFunction<InputT, K1> primaryKeyFn,
        SerializableFunction<InputT, V> valueFn) {
      this.bucketMetadata = bucketMetadata;
      this.primaryKeyFn = primaryKeyFn;
      this.valueFn = valueFn;
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

    static <K1> BucketShardId getBucketShardId(
        K1 key, BucketMetadata<K1, ?, ?> metadata, int shardId) {
      final byte[] keyBytes = metadata.encodeKeyBytes(key, metadata.getKeyCoder());
      return (keyBytes != null)
          ? BucketShardId.of(metadata.getBucketId(keyBytes), shardId)
          : BucketShardId.ofNullKey();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final InputT record = c.element();
      final BucketShardId bucketShardId =
          getBucketShardId(primaryKeyFn.apply(record), bucketMetadata, shardId);
      c.output(KV.of(bucketShardId, valueFn.apply(record)));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
    }
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  private static class ExtractKeysWithCache<K1, V, InputT>
      extends DoFnWithResource<InputT, KV<BucketShardId, V>, Cache<K1, Integer>> {
    private final BucketMetadata<K1, ?, V> bucketMetadata;
    private final int cacheSize;
    private transient int shardId;
    private final SerializableFunction<InputT, K1> primaryKeyFn;
    private final SerializableFunction<InputT, V> valueFn;
    private Counter cacheHits;
    private Counter cacheMisses;

    ExtractKeysWithCache(
        BucketMetadata<K1, ?, V> bucketMetadata,
        SerializableFunction<InputT, K1> primaryKeyFn,
        SerializableFunction<InputT, V> valueFn,
        int cacheSize) {
      this.bucketMetadata = bucketMetadata;
      this.cacheSize = cacheSize;
      this.primaryKeyFn = primaryKeyFn;
      this.valueFn = valueFn;
      cacheHits = Metrics.counter(SortedBucketSink.class, "cacheHits");
      cacheMisses = Metrics.counter(SortedBucketSink.class, "cacheMisses");
    }

    @Override
    public ResourceType getResourceType() {
      return ResourceType.PER_INSTANCE;
    }

    @Override
    public Cache<K1, Integer> createResource() {
      return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    @StartBundle
    public void startBundle() {
      shardId =
          ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) % bucketMetadata.getNumShards();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final InputT record = c.element();
      final K1 primaryKey = primaryKeyFn.apply(record);

      BucketShardId bucketShardId;
      if (primaryKey == null) {
        bucketShardId = BucketShardId.ofNullKey();
      } else {
        bucketShardId =
            Optional.ofNullable(getResource().getIfPresent(primaryKey))
                .map(
                    bucketId -> {
                      cacheHits.inc();
                      return BucketShardId.of(bucketId, shardId);
                    })
                .orElseGet(
                    () -> {
                      cacheMisses.inc();
                      final BucketShardId bId =
                          ExtractBucketAndShardDoFn.getBucketShardId(
                              primaryKey, bucketMetadata, shardId);
                      getResource().put(primaryKey, bId.getBucketId());
                      return bId;
                    });
      }

      c.output(KV.of(bucketShardId, valueFn.apply(record)));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
      builder.add(DisplayData.item("keyCacheSize", cacheSize));
    }
  }

  private static class SortBucketShardDoFn<K1, K2, V>
      extends DoFn<KV<BucketShardId, Iterable<V>>, KV<BucketShardId, Iterable<byte[]>>> {
    private final BufferedExternalSorter.Options sorterOptions;
    private final BucketMetadata<K1, K2, V> bucketMetadata;
    final Coder<V> valueCoder;
    private final Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();
    private final Counter bucketsInitiatedSorting;
    private final Counter bucketsCompletedSorting;

    public SortBucketShardDoFn(
        String transformName,
        BufferedExternalSorter.Options sorterOptions,
        BucketMetadata<K1, K2, V> bucketMetadata,
        Coder<V> valueCoder) {
      this.sorterOptions = sorterOptions;
      this.bucketMetadata = bucketMetadata;
      this.valueCoder = valueCoder;
      this.bucketsInitiatedSorting =
          Metrics.counter(SortedBucketSink.class, transformName + "-bucketsInitiatedSorting");
      this.bucketsCompletedSorting =
          Metrics.counter(SortedBucketSink.class, transformName + "-bucketsCompletedSorting");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final KV<BucketShardId, Iterable<V>> record = c.element();
      final BucketShardId bucketShardId = record.getKey();
      final BufferedExternalSorter primarySorter = BufferedExternalSorter.create(sorterOptions);

      try {
        bucketsInitiatedSorting.inc();
        // sort by primary key
        for (V value : record.getValue()) {
          // TODO is there some better way to avoid this? it's either shuffle or serde here
          final byte[] valueBytes = CoderUtils.encodeToByteArray(valueCoder, value);
          final byte[] keyBytesPrimary = bucketMetadata.getKeyBytesPrimary(value);
          primarySorter.add(
              KV.of(
                  keyBytesPrimary == null
                      ? ExtractBucketAndShardDoFn.NULL_SORT_KEY
                      : keyBytesPrimary,
                  valueBytes));
        }
        final Iterable<KV<byte[], byte[]>> primarySorted = primarySorter.sort();

        if (!bucketMetadata.hasSecondaryKey()) {
          // no secondary sort, so discard key and output
          c.output(KV.of(bucketShardId, Iterables.transform(primarySorted, kv -> kv.getValue())));
        } else {
          // secondary key sort
          byte[] curKey = null;
          ArrayList<byte[]> curKeyValues = new ArrayList<>();
          Iterable<byte[]> out = new ArrayList<>();

          // accumulate each chunk of values associated with a primary key into curKeyValues,
          // sort it by secondary key, and accumulate the sorted values into `out`
          for (KV<byte[], byte[]> kv : primarySorted) {
            byte[] key = kv.getKey();
            if (curKey == null) curKey = key;
            if (bytesComparator.compare(curKey, key) == 0) {
              // same as previously seen key, accumulate value
              curKeyValues.add(kv.getValue());
            } else {
              // this key is new, sort and prepare this chunk of values for output
              out = Iterables.concat(out, secondarySort(curKeyValues));

              // then accumulate the new value and make this key the current key
              curKeyValues = new ArrayList<>();
              curKeyValues.add(kv.getValue());
              curKey = key;
            }
          }
          if (!curKeyValues.isEmpty()) {
            out = Iterables.concat(out, secondarySort(curKeyValues));
          }
          c.output(KV.of(bucketShardId, out));
        }
        bucketsCompletedSorting.inc();
      } catch (IOException e) {
        throw new RuntimeException("Exception sorting buckets", e);
      }
    }

    private Iterable<byte[]> secondarySort(List<byte[]> curKeyValues) throws IOException {
      final BufferedExternalSorter secondarySorter = BufferedExternalSorter.create(sorterOptions);
      curKeyValues.forEach(
          valueBytes -> {
            try {
              V value = CoderUtils.decodeFromByteArray(valueCoder, valueBytes);
              final byte[] keyBytesSecondary = bucketMetadata.getKeyBytesSecondary(value);
              secondarySorter.add(
                  KV.of(
                      keyBytesSecondary != null
                          ? keyBytesSecondary
                          : ExtractBucketAndShardDoFn.NULL_SORT_KEY,
                      valueBytes));
            } catch (IOException e) {
              throw new RuntimeException("Exception sorting buckets", e);
            }
          });
      return Iterables.transform(secondarySorter.sort(), kv2 -> kv2.getValue());
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
      extends PTransform<PCollection<KV<BucketShardId, Iterable<byte[]>>>, WriteResult> {
    private final SMBFilenamePolicy filenamePolicy;
    private final BucketMetadata<?, ?, V> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final ResourceId tempDirectory;
    private final Coder<V> valueCoder;

    WriteOperation(
        SMBFilenamePolicy filenamePolicy,
        BucketMetadata<?, ?, V> bucketMetadata,
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
    public WriteResult expand(PCollection<KV<BucketShardId, Iterable<byte[]>>> input) {
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
                      filenamePolicy.forTempFiles(tempDirectory).getDirectory(),
                      filenamePolicy.forDestination(),
                      bucketMetadata,
                      fileOperations)));
    }
  }

  /** Writes metadata and bucket files to temporary location. */
  static class WriteTempFiles<V>
      extends DoFn<KV<BucketShardId, Iterable<byte[]>>, KV<BucketShardId, ResourceId>> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final Coder<V> valueCoder;

    WriteTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
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
      final Iterable<byte[]> records = c.element().getValue();
      final ResourceId tmpFile = fileAssignment.forBucket(bucketShardId, bucketMetadata);

      LOG.info("Writing sorted-bucket {} to temporary file {}", bucketShardId, tmpFile);
      try (final FileOperations.Writer<V> writer = fileOperations.createWriter(tmpFile)) {
        records.forEach(
            value -> {
              try {
                writer.write(CoderUtils.decodeFromByteArray(valueCoder, value));
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

    private final ResourceId tempDirectory;
    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;

    RenameBuckets(
        ResourceId tempDirectory,
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        FileOperations<V> fileOperations) {
      this.tempDirectory = tempDirectory;
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
                        public void processElement(ProcessContext c) throws IOException {
                          moveFiles(
                              tempDirectory,
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
        ResourceId tempDirectory,
        BucketMetadata<?, ?, ?> bucketMetadata,
        Map<BucketShardId, ResourceId> writtenTmpBuckets,
        FileAssignment dstFileAssignment,
        FileOperations fileOperations,
        Consumer<KV<BucketShardId, ResourceId>> bucketDstConsumer,
        Consumer<ResourceId> metadataDstConsumer,
        boolean writeNullKeyBucket)
        throws IOException {
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
          fileOperations.createWriter(finalDst).close();
          bucketDstConsumer.accept(KV.of(id, finalDst));
        } else {
          srcFiles.add(writtenTmpBuckets.get(id));
          dstFiles.add(finalDst);
          bucketDsts.add(KV.of(id, finalDst));
        }
      }

      LOG.info("Moving {} bucket files into {}", srcFiles.size(), dstFileAssignment.getDirectory());
      // During a failure case, files may have been deleted in an earlier step. Thus
      // we ignore missing files here.
      FileSystems.rename(
          srcFiles,
          dstFiles,
          StandardMoveOptions.IGNORE_MISSING_FILES,
          StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
      bucketDsts.forEach(bucketDstConsumer::accept);

      // Write metadata file last
      final ResourceId metadataDst =
          writeMetadataFile(dstFileAssignment.forMetadata(), bucketMetadata);
      metadataDstConsumer.accept(metadataDst);

      // Some writers, e.g. Parquet might produce extra temporary files like checksum.
      final List<ResourceId> tempFiles = new ArrayList<>();
      final List<MatchResult> matchResults =
          FileSystems.match(Collections.singletonList(tempDirectory.toString() + "*"));
      for (MatchResult result : matchResults) {
        if (result.status() == MatchResult.Status.OK) {
          for (MatchResult.Metadata metadata : result.metadata()) {
            tempFiles.add(metadata.resourceId());
          }
        }
      }
      FileSystems.delete(tempFiles, StandardMoveOptions.IGNORE_MISSING_FILES);

      FileSystems.delete(
          Collections.singletonList(tempDirectory), StandardMoveOptions.IGNORE_MISSING_FILES);
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
      FileSystems.delete(files, StandardMoveOptions.IGNORE_MISSING_FILES);
    } catch (IOException e) {
      cause.addSuppressed(e);
    }
  }

  public static class SortedBucketPreKeyedSink<K, V>
      extends PTransform<PCollection<KV<K, V>>, WriteResult> {

    private final BucketMetadata<K, ?, V> bucketMetadata;
    private final SMBFilenamePolicy filenamePolicy;
    private final ResourceId tempDirectory;
    private final FileOperations<V> fileOperations;
    private final int sorterMemoryMb;
    private final int keyCacheSize;
    private final Coder<V> valueCoder;
    private final boolean verifyKeyExtraction;

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, ?, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        int sorterMemoryMb,
        Coder<V> valueCoder) {
      this(
          bucketMetadata,
          outputDirectory,
          tempDirectory,
          filenameSuffix,
          fileOperations,
          sorterMemoryMb,
          valueCoder,
          true);
    }

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, ?, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        int sorterMemoryMb,
        Coder<V> valueCoder,
        boolean verifyKeyExtraction) {
      this(
          bucketMetadata,
          outputDirectory,
          tempDirectory,
          filenameSuffix,
          fileOperations,
          sorterMemoryMb,
          valueCoder,
          verifyKeyExtraction,
          0);
    }

    public SortedBucketPreKeyedSink(
        BucketMetadata<K, ?, V> bucketMetadata,
        ResourceId outputDirectory,
        ResourceId tempDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        int sorterMemoryMb,
        Coder<V> valueCoder,
        boolean verifyKeyExtraction,
        int keyCacheSize) {
      this.bucketMetadata = bucketMetadata;
      this.filenamePolicy =
          new SMBFilenamePolicy(
              outputDirectory, bucketMetadata.getFilenamePrefix(), filenameSuffix);
      this.tempDirectory = tempDirectory;
      this.fileOperations = fileOperations;
      this.sorterMemoryMb = sorterMemoryMb;
      this.keyCacheSize = keyCacheSize;
      this.verifyKeyExtraction = verifyKeyExtraction;
      this.valueCoder = valueCoder;
    }

    @Override
    public final WriteResult expand(PCollection<KV<K, V>> input) {
      // @Todo: should we allow windowed writes?
      Preconditions.checkArgument(
          input.isBounded() == IsBounded.BOUNDED,
          "SortedBucketSink cannot be applied to a non-bounded PCollection");
      final PCollection<KV<BucketShardId, V>> bucketedInput =
          input.apply(
              "ExtractBucketAndShards",
              ParDo.of(ExtractBucketAndShardDoFn.preKeyed(bucketMetadata, keyCacheSize)));

      if (verifyKeyExtraction) {
        input
            .apply("VerifySample", Sample.any(100))
            .apply(
                "VerifyKeyFn",
                ParDo.of(
                    new DoFn<KV<K, V>, Void>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        final K key = bucketMetadata.extractKeyPrimary(c.element().getValue());
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
          valueCoder,
          sorterMemoryMb,
          filenamePolicy,
          fileOperations,
          bucketMetadata,
          tempDirectory);
    }
  }
}
