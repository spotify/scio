/*
 * Copyright 2020 Spotify AB.
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

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.RenameBuckets;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that encapsulates both a {@link SortedBucketSource} and {@link
 * SortedBucketSink} operation, with a user-supplied transform function mapping merged {@link
 * CoGbkResult}s to their final writable outputs.
 *
 * @param <FinalKeyT>
 * @param <FinalValueT>
 */
public class SortedBucketTransform<FinalKeyT, FinalValueT> extends PTransform<PBegin, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketTransform.class);
  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.33;

  private final BucketSource<FinalKeyT> bucketSource;
  private final DoFn<Iterable<MergedBucket>, KV<BucketShardId, ResourceId>> finalizeBuckets;
  private final ParDo.SingleOutput<BucketItem, MergedBucket> doFn;

  public SortedBucketTransform(
      Class<FinalKeyT> finalKeyClass,
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      TransformFn<FinalKeyT, FinalValueT> transformFn,
      TransformFnWithSideInputContext<FinalKeyT, FinalValueT> sideInputTransformFn,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Iterable<PCollectionView<?>> sides,
      NewBucketMetadataFn<FinalKeyT, FinalValueT> newBucketMetadataFn,
      FileOperations<FinalValueT> fileOperations,
      String filenameSuffix,
      String filenamePrefix) {
    Preconditions.checkNotNull(outputDirectory, "outputDirectory is not set");
    Preconditions.checkState(
        !((transformFn == null) && (sideInputTransformFn == null)), // at least one defined
        "At least one of transformFn and sideInputTransformFn must be set");
    Preconditions.checkState(
        !((transformFn != null) && (sideInputTransformFn != null)), // only one defined
        "At most one of transformFn and sideInputTransformFn may be set");
    if (sideInputTransformFn != null) {
      Preconditions.checkNotNull(sides, "If using sideInputTransformFn, sides must not be null");
    }

    final SMBFilenamePolicy filenamePolicy =
        new SMBFilenamePolicy(outputDirectory, filenamePrefix, filenameSuffix);
    final SourceSpec<FinalKeyT> sourceSpec = SourceSpec.from(finalKeyClass, sources);
    bucketSource = new BucketSource<>(sources, targetParallelism, 1, 0, sourceSpec, -1);
    final FileAssignment fileAssignment = filenamePolicy.forTempFiles(tempDirectory);

    finalizeBuckets =
        new FinalizeTransformedBuckets<>(
            fileAssignment.getDirectory(),
            fileOperations,
            newBucketMetadataFn,
            filenamePolicy.forDestination(),
            sourceSpec.hashType);

    final Distribution dist = Metrics.distribution(getName(), getName() + "-KeyGroupSize");
    if (transformFn != null) {
      this.doFn =
          ParDo.of(
              new TransformWithNoSides<>(
                  sources, sourceSpec, fileAssignment, fileOperations, transformFn, dist));
    } else {
      this.doFn =
          ParDo.of(
                  new TransformWithSides<>(
                      sources,
                      sourceSpec,
                      fileAssignment,
                      fileOperations,
                      sideInputTransformFn,
                      dist))
              .withSideInputs(sides);
    }
  }

  @Override
  public final WriteResult expand(final PBegin begin) {
    return WriteResult.fromTuple(
        begin
            .getPipeline()
            // outputs bucket offsets for the various SMB readers
            .apply("BucketOffsets", Read.from(bucketSource))
            .apply("MergeBuckets", this.doFn)
            .apply(Filter.by(Objects::nonNull))
            .apply(Group.globally())
            .apply(
                "FinalizeTempFiles",
                ParDo.of(finalizeBuckets)
                    .withOutputTags(
                        FinalizeTransformedBuckets.BUCKETS_TAG,
                        TupleTagList.of(FinalizeTransformedBuckets.METADATA_TAG))));
  }

  @FunctionalInterface
  public interface TransformFn<KeyT, ValueT> extends Serializable {
    void writeTransform(
        KV<KeyT, CoGbkResult> keyGroup, SerializableConsumer<ValueT> outputConsumer);
  }

  @FunctionalInterface
  public interface TransformFnWithSideInputContext<KeyT, ValueT> extends Serializable {
    void writeTransform(
        KV<KeyT, CoGbkResult> keyGroup,
        DoFn<BucketItem, MergedBucket>.ProcessContext c,
        SerializableConsumer<ValueT> outputConsumer,
        BoundedWindow window);
  }

  public interface SerializableConsumer<ValueT> extends Consumer<ValueT>, Serializable {}

  public interface NewBucketMetadataFn<K, V> extends Serializable {
    BucketMetadata<K, V> createMetadata(int numBuckets, int numShards, HashType hashType)
        throws CannotProvideCoderException, NonDeterministicException;
  }

  private static class FinalizeTransformedBuckets<FinalValueT>
      extends DoFn<Iterable<MergedBucket>, KV<BucketShardId, ResourceId>> {
    private final ResourceId tempDirectory;
    private final FileOperations<FinalValueT> fileOperations;
    private final NewBucketMetadataFn<?, ?> newBucketMetadataFn;
    private final FileAssignment dstFileAssignment;
    private final HashType hashType;

    static final TupleTag<KV<BucketShardId, ResourceId>> BUCKETS_TAG =
        new TupleTag<>("writtenBuckets");
    static final TupleTag<ResourceId> METADATA_TAG = new TupleTag<>("writtenMetadata");

    public FinalizeTransformedBuckets(
        ResourceId tempDirectory,
        FileOperations<FinalValueT> fileOperations,
        NewBucketMetadataFn<?, ?> newBucketMetadataFn,
        FileAssignment dstFileAssignment,
        HashType hashType) {
      this.tempDirectory = tempDirectory;
      this.fileOperations = fileOperations;
      this.newBucketMetadataFn = newBucketMetadataFn;
      this.dstFileAssignment = dstFileAssignment;
      this.hashType = hashType;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      final Iterator<MergedBucket> mergedBuckets = c.element().iterator();
      final Map<BucketShardId, ResourceId> writtenBuckets = new HashMap<>();

      BucketMetadata<?, ?> bucketMetadata = null;
      while (mergedBuckets.hasNext()) {
        final MergedBucket bucket = mergedBuckets.next();
        if (bucketMetadata == null) {
          try {
            bucketMetadata =
                newBucketMetadataFn.createMetadata(bucket.totalNumBuckets, 1, hashType);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        writtenBuckets.put(BucketShardId.of(bucket.bucketId, 0), bucket.destination);
      }

      RenameBuckets.moveFiles(
          tempDirectory,
          bucketMetadata,
          writtenBuckets,
          dstFileAssignment,
          fileOperations,
          bucketDst -> c.output(BUCKETS_TAG, bucketDst),
          metadataDst -> c.output(METADATA_TAG, metadataDst),
          false); // Don't include null-key bucket in output
    }
  }

  private static class OutputCollector<ValueT> implements SerializableConsumer<ValueT> {
    private final Writer<ValueT> writer;

    OutputCollector(Writer<ValueT> writer) {
      this.writer = writer;
    }

    void onComplete() {
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException("Closing writer failed: ", e);
      }
    }

    @Override
    public void accept(ValueT t) {
      try {
        writer.write(t);
      } catch (IOException e) {
        throw new RuntimeException("Write of element " + t + " failed: ", e);
      }
    }
  }

  public static class MergedBucket implements Serializable {
    final ResourceId destination;
    final int bucketId;
    final int totalNumBuckets;

    MergedBucket(Integer bucketId, ResourceId destination, Integer totalNumBuckets) {
      this.destination = destination;
      this.bucketId = bucketId;
      this.totalNumBuckets = totalNumBuckets;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MergedBucket that = (MergedBucket) o;
      return Objects.equals(destination, that.destination)
          && Objects.equals(bucketId, that.bucketId)
          && Objects.equals(totalNumBuckets, that.totalNumBuckets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(destination, bucketId, totalNumBuckets);
    }
  }

  private static class BucketSource<FinalKeyT> extends BoundedSource<BucketItem> {
    // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
    // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.

    private final List<SortedBucketSource.BucketedInput<?, ?>> sources;
    private final TargetParallelism targetParallelism;
    private final SourceSpec<FinalKeyT> sourceSpec;

    private final int effectiveParallelism;
    private final int bucketOffsetId;
    private long estimatedSizeBytes;

    public BucketSource(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        TargetParallelism targetParallelism,
        int effectiveParallelism,
        int bucketOffsetId,
        SourceSpec<FinalKeyT> sourceSpec,
        long estimatedSizeBytes) {
      this.sources = sources;
      this.targetParallelism = targetParallelism;
      this.effectiveParallelism = effectiveParallelism;
      this.bucketOffsetId = bucketOffsetId;
      this.sourceSpec = sourceSpec;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    public BucketSource<FinalKeyT> split(
        int bucketOffsetId, int adjustedParallelism, long estimatedSizeBytes) {
      return new BucketSource<>(
          sources,
          targetParallelism,
          adjustedParallelism,
          bucketOffsetId,
          sourceSpec,
          estimatedSizeBytes);
    }

    @Override
    public List<? extends BoundedSource<BucketItem>> split(
        final long desiredBundleSizeBytes, final PipelineOptions options) throws Exception {

      final int numSplits =
          SortedBucketSource.getNumSplits(
              sourceSpec,
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
          .map(
              i ->
                  this.split(
                      bucketOffsetId + (i * effectiveParallelism), totalParallelism, estSplitSize))
          .collect(Collectors.toList());
    }

    @Override
    public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
      if (estimatedSizeBytes == -1) {
        estimatedSizeBytes =
            sources
                .parallelStream()
                .mapToLong(SortedBucketSource.BucketedInput::getOrSampleByteSize)
                .sum();
        LOG.info("Estimated byte size is " + estimatedSizeBytes);
      }
      return estimatedSizeBytes;
    }

    @Override
    public Coder<BucketItem> getOutputCoder() {
      return NullableCoder.of(SerializableCoder.of(BucketItem.class));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
    }

    @Override
    public BoundedReader<BucketItem> createReader(final PipelineOptions options)
        throws IOException {
      return new BucketReader(this, bucketOffsetId, effectiveParallelism);
    }
  }

  private abstract static class TransformDoFn<FinalKeyT, FinalValueT>
      extends DoFn<BucketItem, MergedBucket> {
    protected final SMBFilenamePolicy.FileAssignment fileAssignment;
    protected final FileOperations<FinalValueT> fileOperations;
    protected final List<SortedBucketSource.BucketedInput<?, ?>> sources;
    protected final Distribution keyGroupSize;
    protected final SourceSpec<FinalKeyT> sourceSpec;

    protected TransformDoFn(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        Distribution keyGroupSize) {
      this.fileAssignment = fileAssignment;
      this.fileOperations = fileOperations;
      this.sources = sources;
      this.keyGroupSize = keyGroupSize;
      this.sourceSpec = sourceSpec;
    }

    protected abstract void outputTransform(
        KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        ProcessContext context,
        OutputCollector<FinalValueT> outputCollector,
        BoundedWindow window);

    @ProcessElement
    public void processElement(
        @Element BucketItem e,
        OutputReceiver<MergedBucket> out,
        ProcessContext context,
        BoundedWindow window) {
      int bucketId = e.bucketOffsetId;
      int effectiveParallelism = e.effectiveParallelism;

      ResourceId dst =
          fileAssignment.forBucket(BucketShardId.of(bucketId, 0), effectiveParallelism, 1);
      OutputCollector<FinalValueT> outputCollector;
      try {
        outputCollector = new OutputCollector<>(fileOperations.createWriter(dst));
      } catch (IOException err) {
        throw new RuntimeException("Failed to create file writer for transformed output", err);
      }

      final MultiSourceKeyGroupReader<FinalKeyT> iter =
          new MultiSourceKeyGroupReader<>(
              sources,
              sourceSpec,
              keyGroupSize,
              false,
              bucketId,
              effectiveParallelism,
              context.getPipelineOptions());
      while (true) {
        try {
          KV<FinalKeyT, CoGbkResult> mergedKeyGroup = iter.readNext();
          if (mergedKeyGroup == null) break;
          outputTransform(mergedKeyGroup, context, outputCollector, window);

          // exhaust iterators if necessary before moving on to the next key group:
          // for example, if not every element was needed in the transformFn
          sources.forEach(
              source -> {
                final Iterable<?> maybeUnfinishedIt =
                    mergedKeyGroup.getValue().getAll(source.getTupleTag());
                if (SortedBucketSource.TraversableOnceIterable.class.isAssignableFrom(
                    maybeUnfinishedIt.getClass())) {
                  ((SortedBucketSource.TraversableOnceIterable<?>) maybeUnfinishedIt)
                      .ensureExhausted();
                }
              });
        } catch (Exception ex) {
          throw new RuntimeException("Failed to write merged key group", ex);
        }
      }
      outputCollector.onComplete();
      out.output(new MergedBucket(bucketId, dst, effectiveParallelism));
    }
  }

  private static class TransformWithNoSides<FinalKeyT, FinalValueT>
      extends TransformDoFn<FinalKeyT, FinalValueT> {
    private final TransformFn<FinalKeyT, FinalValueT> transformFn;

    public TransformWithNoSides(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        TransformFn<FinalKeyT, FinalValueT> transformFn,
        Distribution keyGroupSize) {
      super(sources, sourceSpec, fileAssignment, fileOperations, keyGroupSize);
      this.transformFn = transformFn;
    }

    @Override
    protected void outputTransform(
        final KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        final ProcessContext context,
        final OutputCollector<FinalValueT> outputCollector,
        final BoundedWindow window) {
      transformFn.writeTransform(mergedKeyGroup, outputCollector);
    }
  }

  private static class TransformWithSides<FinalKeyT, FinalValueT>
      extends TransformDoFn<FinalKeyT, FinalValueT> {
    private final TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn;

    public TransformWithSides(
        List<SortedBucketSource.BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        SMBFilenamePolicy.FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        TransformFnWithSideInputContext<FinalKeyT, FinalValueT> transformFn,
        Distribution keyGroupSize) {
      super(sources, sourceSpec, fileAssignment, fileOperations, keyGroupSize);
      this.transformFn = transformFn;
    }

    @Override
    protected void outputTransform(
        final KV<FinalKeyT, CoGbkResult> mergedKeyGroup,
        final ProcessContext context,
        final OutputCollector<FinalValueT> outputCollector,
        final BoundedWindow window) {
      transformFn.writeTransform(mergedKeyGroup, context, outputCollector, window);
    }
  }

  private static class BucketReader extends BoundedSource.BoundedReader<BucketItem> {
    private final BoundedSource<BucketItem> currentSource;
    private boolean started;
    // there is only ever a single item in this source
    @Nullable private BucketItem next;

    public BucketReader(
        BoundedSource<BucketItem> initialSource, int bucketOffsetId, int effectiveParallelism) {
      this.currentSource = initialSource;
      this.started = false;
      this.next = new BucketItem(bucketOffsetId, effectiveParallelism);
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (!started && next != null) {
        started = true;
        return true;
      } else {
        next = null;
        return false;
      }
    }

    @Override
    public BucketItem getCurrent() throws NoSuchElementException {
      if (!started || next == null) {
        throw new NoSuchElementException();
      }
      return next;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<BucketItem> getCurrentSource() {
      return currentSource;
    }
  }

  public static class BucketItem implements Serializable {
    final int bucketOffsetId;
    final int effectiveParallelism;

    public BucketItem(Integer bucketOffsetId, Integer effectiveParallelism) {
      this.bucketOffsetId = bucketOffsetId;
      this.effectiveParallelism = effectiveParallelism;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BucketItem that = (BucketItem) o;
      return Objects.equals(bucketOffsetId, that.bucketOffsetId)
          && Objects.equals(effectiveParallelism, that.effectiveParallelism);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bucketOffsetId, effectiveParallelism);
    }
  }
}
