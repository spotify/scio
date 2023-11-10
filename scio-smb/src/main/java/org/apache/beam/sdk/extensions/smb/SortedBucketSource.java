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
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
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
 * @param <KeyType> the type of the result keys. Sources can have different key types as long as
 *     they can all be decoded as this type (see: {@link BucketMetadata#getKeyCoder()} and {@link
 *     BucketMetadata#getKeyCoderSecondary()}) and are bucketed using the same {@code byte[]}
 *     representation (see: {@link BucketMetadata#getKeyBytesPrimary(Object)} and {@link
 *     BucketMetadata#getKeyBytesSecondary(Object)}).
 */
public abstract class SortedBucketSource<KeyType> extends BoundedSource<KV<KeyType, CoGbkResult>> {
  public enum Keying {
    PRIMARY,
    PRIMARY_AND_SECONDARY
  }

  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.5;
  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSource.class);
  private static final AtomicInteger metricsId = new AtomicInteger(1);

  protected final List<BucketedInput<?>> sources;
  protected final TargetParallelism targetParallelism;
  protected final int effectiveParallelism;
  protected final int bucketOffsetId;
  protected SourceSpec sourceSpec;
  protected final Distribution keyGroupSize;
  protected Long estimatedSizeBytes;
  protected final String metricsKey;

  public SortedBucketSource(List<BucketedInput<?>> sources) {
    this(sources, TargetParallelism.auto());
  }

  public SortedBucketSource(List<BucketedInput<?>> sources, TargetParallelism targetParallelism) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(sources, targetParallelism, 0, 1, null);
  }

  public SortedBucketSource(
      List<BucketedInput<?>> sources, TargetParallelism targetParallelism, String metricsKey) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(sources, targetParallelism, 0, 1, metricsKey);
  }

  private SortedBucketSource(
      List<BucketedInput<?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism,
      String metricsKey) {
    this(sources, targetParallelism, bucketOffsetId, effectiveParallelism, metricsKey, null);
  }

  protected SortedBucketSource(
      List<BucketedInput<?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism,
      String metricsKey,
      Long estimatedSizeBytes) {
    this.sources = sources;
    this.targetParallelism =
        targetParallelism == null ? TargetParallelism.auto() : targetParallelism;
    this.bucketOffsetId = bucketOffsetId;
    this.effectiveParallelism = effectiveParallelism;
    this.metricsKey = metricsKey == null ? getDefaultMetricsKey() : metricsKey;
    this.keyGroupSize =
        Metrics.distribution(SortedBucketSource.class, this.metricsKey + "-KeyGroupSize");
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  protected abstract Coder<KeyType> keyTypeCoder();

  protected abstract Function<SortedBucketIO.ComparableKeyBytes, KeyType> toKeyFn();

  /** @return A split source of the implementing subtype */
  protected abstract SortedBucketSource<KeyType> createSplitSource(
      int splitNum, int totalParallelism, long estSplitSize);

  protected abstract Comparator<SortedBucketIO.ComparableKeyBytes> comparator();

  private static String getDefaultMetricsKey() {
    return "SortedBucketSource{" + metricsId.getAndAdd(1) + "}";
  }

  @VisibleForTesting
  int getBucketOffset() {
    return bucketOffsetId;
  }

  @VisibleForTesting
  int getEffectiveParallelism() {
    return effectiveParallelism;
  }

  protected SourceSpec getOrComputeSourceSpec() {
    if (this.sourceSpec == null) this.sourceSpec = SourceSpec.from(sources);
    return this.sourceSpec;
  }

  protected CoGbkResultSchema coGbkResultSchema() {
    return CoGbkResultSchema.of(
        sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList()));
  }

  @Override
  public Coder<KV<KeyType, CoGbkResult>> getOutputCoder() {
    return KvCoder.of(
        keyTypeCoder(),
        CoGbkResult.CoGbkResultCoder.of(
            coGbkResultSchema(),
            UnionCoder.of(sources.stream().map(i -> i.getCoder()).collect(Collectors.toList()))));
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
    builder.add(DisplayData.item("metricsKey", metricsKey));
  }

  @Override
  public List<? extends BoundedSource<KV<KeyType, CoGbkResult>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    final int numSplits =
        getNumSplits(
            getOrComputeSourceSpec(),
            effectiveParallelism,
            targetParallelism,
            getEstimatedSizeBytes(options),
            desiredBundleSizeBytes,
            DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR);
    final long estSplitSize = estimatedSizeBytes / numSplits;
    final DecimalFormat sizeFormat = new DecimalFormat("0.00");
    LOG.info(
        "Parallelism was adjusted by {}splitting source of size {} MB into {} source(s) of size {} MB",
        effectiveParallelism > 1 ? "further " : "",
        sizeFormat.format(estimatedSizeBytes / 1000000.0),
        numSplits,
        sizeFormat.format(estSplitSize / 1000000.0));
    final int totalParallelism = numSplits * effectiveParallelism;
    return IntStream.range(0, numSplits)
        .boxed()
        .map(splitNum -> createSplitSource(splitNum, totalParallelism, estSplitSize))
        .collect(Collectors.toList());
  }

  static int getNumSplits(
      SourceSpec sourceSpec,
      int effectiveParallelism,
      TargetParallelism targetParallelism,
      long estimatedSizeBytes,
      long desiredSizeBytes,
      double adjustmentFactor) {
    if (effectiveParallelism == sourceSpec.greatestNumBuckets) {
      LOG.info("Parallelism is already maxed, can't split further.");
      return 1;
    }
    if (!targetParallelism.isAuto()) {
      int p = sourceSpec.getParallelism(targetParallelism);
      LOG.info("Splitting using specified parallelism: " + targetParallelism);
      return p;
    }
    desiredSizeBytes *= adjustmentFactor;
    int fanout = (int) Math.round(estimatedSizeBytes / (desiredSizeBytes * 1.0));
    if (fanout <= 1) {
      LOG.info("Desired byte size is <= total input size, can't split further.");
      return 1;
    }
    // round up to nearest power of 2, bounded by greatest # of buckets
    return Math.min(Integer.highestOneBit(fanout - 1) * 2, sourceSpec.greatestNumBuckets);
  }

  // `getEstimatedSizeBytes` is called frequently by Dataflow, don't recompute every time
  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (estimatedSizeBytes == null) {
      estimatedSizeBytes =
          sources.parallelStream().mapToLong(BucketedInput::getOrSampleByteSize).sum();
      LOG.info("Estimated byte size is " + estimatedSizeBytes);
    }
    return estimatedSizeBytes;
  }

  @Override
  public BoundedReader<KV<KeyType, CoGbkResult>> createReader(final PipelineOptions options)
      throws IOException {
    // get any arbitrary metadata to be able to rehash keys into buckets
    BucketMetadata<?, ?, ?> someArbitraryBucketMetadata =
        sources.get(0).getSourceMetadata().mapping.values().stream().findAny().get().metadata;
    return new MergeBucketsReader<>(
        new MultiSourceKeyGroupReader<KeyType>(
            sources,
            toKeyFn(),
            coGbkResultSchema(),
            someArbitraryBucketMetadata,
            comparator(),
            keyGroupSize,
            true,
            bucketOffsetId,
            effectiveParallelism,
            options),
        this);
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBucketsReader<KeyType> extends BoundedReader<KV<KeyType, CoGbkResult>> {
    private final SortedBucketSource<KeyType> currentSource;
    private final MultiSourceKeyGroupReader<KeyType> iter;
    private KV<KeyType, CoGbkResult> next = null;

    MergeBucketsReader(
        MultiSourceKeyGroupReader<KeyType> iter, SortedBucketSource<KeyType> currentSource) {
      this.currentSource = currentSource;
      this.iter = iter;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public KV<KeyType, CoGbkResult> getCurrent() throws NoSuchElementException {
      if (next == null) throw new NoSuchElementException();
      return next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean advance() throws IOException {
      next = iter.readNext();
      return next != null;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<KV<KeyType, CoGbkResult>> getCurrentSource() {
      return currentSource;
    }
  }

  static class TraversableOnceIterable<V> implements Iterable<V> {
    private final Iterator<V> underlying;
    private boolean called = false;

    TraversableOnceIterable(Iterator<V> underlying) {
      this.underlying = underlying;
    }

    @Override
    public Iterator<V> iterator() {
      Preconditions.checkArgument(
          !called,
          "CoGbkResult .iterator() can only be called once. To be re-iterable, it must be materialized as a List.");
      called = true;
      return underlying;
    }

    void ensureExhausted() {
      this.underlying.forEachRemaining(v -> {});
    }
  }

  public static class PrimaryKeyedBucketedInput<V> extends BucketedInput<V> {
    public PrimaryKeyedBucketedInput(
        TupleTag<V> tupleTag,
        List<String> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Predicate<V> predicate) {
      this(
          tupleTag,
          inputDirectories.stream()
              .collect(
                  Collectors.toMap(
                      Functions.identity(), dir -> KV.of(filenameSuffix, fileOperations))),
          predicate);
    }

    public PrimaryKeyedBucketedInput(
        TupleTag<V> tupleTag,
        Map<String, KV<String, FileOperations<V>>> directories,
        Predicate<V> predicate) {
      super(Keying.PRIMARY, tupleTag, directories, predicate);
    }

    public SourceMetadata<V> getSourceMetadata() {
      if (sourceMetadata == null)
        sourceMetadata = BucketMetadataUtil.get().getPrimaryKeyedSourceMetadata(directories);
      return sourceMetadata;
    }
  }

  public static class PrimaryAndSecondaryKeyedBucktedInput<V> extends BucketedInput<V> {
    public PrimaryAndSecondaryKeyedBucktedInput(
        TupleTag<V> tupleTag,
        List<String> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Predicate<V> predicate) {
      this(
          tupleTag,
          inputDirectories.stream()
              .collect(
                  Collectors.toMap(
                      Functions.identity(), dir -> KV.of(filenameSuffix, fileOperations))),
          predicate);
    }

    public PrimaryAndSecondaryKeyedBucktedInput(
        TupleTag<V> tupleTag,
        Map<String, KV<String, FileOperations<V>>> directories,
        Predicate<V> predicate) {
      super(Keying.PRIMARY_AND_SECONDARY, tupleTag, directories, predicate);
    }

    public SourceMetadata<V> getSourceMetadata() {
      if (sourceMetadata == null)
        sourceMetadata =
            BucketMetadataUtil.get().getPrimaryAndSecondaryKeyedSourceMetadata(directories);
      return sourceMetadata;
    }
  }

  /**
   * Abstracts a sorted-bucket input to {@link SortedBucketSource} written by {@link
   * SortedBucketSink}.
   *
   * @param <V> the type of the values in a bucket
   */
  public abstract static class BucketedInput<V> implements Serializable {
    private static final Pattern BUCKET_PATTERN = Pattern.compile("(\\d+)-of-(\\d+)");

    protected TupleTag<V> tupleTag;
    protected Map<ResourceId, KV<String, FileOperations<V>>> directories;
    protected Predicate<V> predicate;
    protected Keying keying;
    // lazy, internal checks depend on what kind of iteration is requested
    protected transient SourceMetadata<V> sourceMetadata = null; // lazy

    public static <V> BucketedInput<V> of(
        Keying keying,
        TupleTag<V> tupleTag,
        List<String> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Predicate<V> predicate) {
      if (keying == Keying.PRIMARY)
        return new PrimaryKeyedBucketedInput<>(
            tupleTag, inputDirectories, filenameSuffix, fileOperations, predicate);
      return new PrimaryAndSecondaryKeyedBucktedInput<>(
          tupleTag, inputDirectories, filenameSuffix, fileOperations, predicate);
    }

    public BucketedInput(
        Keying keying,
        TupleTag<V> tupleTag,
        List<String> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Predicate<V> predicate) {
      this(
          keying,
          tupleTag,
          inputDirectories.stream()
              .collect(
                  Collectors.toMap(
                      Functions.identity(), dir -> KV.of(filenameSuffix, fileOperations))),
          predicate);
    }

    public BucketedInput(
        Keying keying,
        TupleTag<V> tupleTag,
        Map<String, KV<String, FileOperations<V>>> directories,
        Predicate<V> predicate) {
      final Set<TypeDescriptor<V>> coderTypes =
          directories.values().stream()
              .map(f -> f.getValue().getCoder().getEncodedTypeDescriptor())
              .collect(Collectors.toSet());

      Preconditions.checkArgument(
          coderTypes.size() == 1,
          "All FileOperations Coders must use the same encoding type; found: " + coderTypes);

      this.keying = keying;
      this.tupleTag = tupleTag;
      this.directories =
          directories.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      e -> FileSystems.matchNewResource(e.getKey(), true), Map.Entry::getValue));
      this.predicate = predicate;
    }

    public abstract SourceMetadata<V> getSourceMetadata();

    public TupleTag<V> getTupleTag() {
      return tupleTag;
    }

    public Predicate<V> getPredicate() {
      return predicate;
    }

    public Coder<V> getCoder() {
      return directories.entrySet().iterator().next().getValue().getValue().getCoder();
    }

    static CoGbkResultSchema schemaOf(List<BucketedInput<?>> sources) {
      return CoGbkResultSchema.of(
          sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList()));
    }

    private static List<Metadata> sampleDirectory(ResourceId directory, String filepattern) {
      try {
        return FileSystems.match(
                directory.resolve(filepattern, StandardResolveOptions.RESOLVE_FILE).toString())
            .metadata();
      } catch (FileNotFoundException e) {
        return Collections.emptyList();
      } catch (IOException e) {
        throw new RuntimeException("Exception fetching metadata for " + directory, e);
      }
    }

    long getOrSampleByteSize() {
      return directories
          .entrySet()
          .parallelStream()
          .mapToLong(
              entry -> {
                // Take at most 10 buckets from the directory to sample
                // Check for single-shard filenames template first, then multi-shard
                List<Metadata> sampledFiles =
                    sampleDirectory(entry.getKey(), "*-0000?-of-?????" + entry.getValue().getKey());
                if (sampledFiles.isEmpty()) {
                  sampledFiles =
                      sampleDirectory(
                          entry.getKey(),
                          "*-0000?-of-*-shard-00000-of-?????" + entry.getValue().getKey());
                }

                int numBuckets = 0;
                long sampledBytes = 0L;
                final Set<String> seenBuckets = new HashSet<>();

                for (Metadata metadata : sampledFiles) {
                  final Matcher matcher =
                      BUCKET_PATTERN.matcher(metadata.resourceId().getFilename());
                  if (!matcher.find()) {
                    throw new RuntimeException(
                        "Couldn't match bucket information from filename: "
                            + metadata.resourceId().getFilename());
                  }
                  seenBuckets.add(matcher.group(1));
                  if (numBuckets == 0) {
                    numBuckets = Integer.parseInt(matcher.group(2));
                  }
                  sampledBytes += metadata.sizeBytes();
                }
                if (numBuckets == 0) {
                  throw new IllegalArgumentException(
                      "Directory " + entry.getKey() + " has no bucket files");
                }
                if (seenBuckets.size() < numBuckets) {
                  return (long) (sampledBytes * (numBuckets / (seenBuckets.size() * 1.0)));
                } else {
                  return sampledBytes;
                }
              })
          .sum();
    }

    public KeyGroupIterator<V> createIterator(
        int bucketId, int targetParallelism, PipelineOptions options) {
      SourceMetadata<V> sourceMetadata = getSourceMetadata();
      final Comparator<SortedBucketIO.ComparableKeyBytes> keyComparator =
          (keying == Keying.PRIMARY)
              ? new SortedBucketIO.PrimaryKeyComparator()
              : new SortedBucketIO.PrimaryAndSecondaryKeyComparator();

      final SortedBucketOptions opts = options.as(SortedBucketOptions.class);
      final int bufferSize = opts.getSortedBucketReadBufferSize();
      final int diskBufferMb = opts.getSortedBucketReadDiskBufferMb();
      FileOperations.setDiskBufferMb(diskBufferMb);

      final List<Iterator<KV<SortedBucketIO.ComparableKeyBytes, V>>> iterators = new ArrayList<>();
      sourceMetadata.mapping.forEach(
          (dir, value) -> {
            final int numBuckets = value.metadata.getNumBuckets();
            final int numShards = value.metadata.getNumShards();
            final Function<V, SortedBucketIO.ComparableKeyBytes> keyFn =
                (keying == Keying.PRIMARY)
                    ? value.metadata::primaryComparableKeyBytes
                    : value.metadata::primaryAndSecondaryComparableKeyBytes;
            for (int i = (bucketId % numBuckets); i < numBuckets; i += targetParallelism) {
              for (int j = 0; j < numShards; j++) {
                ResourceId file =
                    value.fileAssignment.forBucket(BucketShardId.of(i, j), numBuckets, numShards);
                try {
                  Iterator<KV<SortedBucketIO.ComparableKeyBytes, V>> iterator =
                      Iterators.transform(
                          directories.get(dir).getValue().iterator(file),
                          v -> KV.of(keyFn.apply(v), v));
                  Iterator<KV<SortedBucketIO.ComparableKeyBytes, V>> out =
                      (bufferSize > 0) ? new BufferedIterator<>(iterator, bufferSize) : iterator;
                  iterators.add(out);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
          });
      return new KeyGroupIterator<>(iterators, keyComparator);
    }

    @Override
    public String toString() {
      List<ResourceId> inputDirectories = new ArrayList<>(directories.keySet());
      return String.format(
          "BucketedInput[tupleTag=%s, inputDirectories=[%s]]",
          tupleTag.getId(),
          inputDirectories.size() > 5
              ? inputDirectories.subList(0, 4)
                  + "..."
                  + inputDirectories.get(inputDirectories.size() - 1)
              : inputDirectories);
    }

    // Not all instance members can be natively serialized, so override writeObject/readObject
    // using Coders for each type
    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream outStream) throws IOException {
      SerializableCoder.of(TupleTag.class).encode(tupleTag, outStream);
      outStream.writeInt(directories.size());
      for (Map.Entry<ResourceId, KV<String, FileOperations<V>>> entry : directories.entrySet()) {
        ResourceIdCoder.of().encode(entry.getKey(), outStream);
        outStream.writeObject(entry.getValue());
      }
      outStream.writeObject(predicate);
      outStream.writeObject(keying);
      outStream.flush();
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inStream) throws ClassNotFoundException, IOException {
      this.tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
      final int numDirectories = inStream.readInt();
      this.directories = new HashMap<>();
      for (int i = 0; i < numDirectories; i++) {
        directories.put(
            ResourceIdCoder.of().decode(inStream),
            (KV<String, FileOperations<V>>) inStream.readObject());
      }
      this.predicate = (Predicate<V>) inStream.readObject();
      this.keying = (Keying) inStream.readObject();
    }
  }

  /**
   * Filter predicate when building the {{@code Iterable<T>}} in {{@link CoGbkResult}}.
   *
   * <p>First argument {{@code List<T>}} is the work in progress buffer for the current key. Second
   * argument {{@code T}} is the next element to be added. Return true to accept the next element,
   * or false to reject it.
   */
  public interface Predicate<T> extends BiFunction<List<T>, T, Boolean>, Serializable {}
}
