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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.MergeBucketsReader;
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
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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
  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.33;

  private final MergeAndWriteBucketsSource<FinalKeyT, FinalValueT> boundedSource;
  private final FinalizeTransformedBuckets<FinalValueT> finalizeBuckets;

  public SortedBucketTransform(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      TransformFn<FinalKeyT, FinalValueT> transformFn,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      NewBucketMetadataFn<FinalKeyT, FinalValueT> newBucketMetadataFn,
      FileOperations<FinalValueT> fileOperations,
      String filenameSuffix,
      String filenamePrefix) {
    final SMBFilenamePolicy filenamePolicy =
        new SMBFilenamePolicy(outputDirectory, filenamePrefix, filenameSuffix);
    final SourceSpec<FinalKeyT> sourceSpec = SourceSpec.from(finalKeyClass, sources);

    boundedSource =
        new MergeAndWriteBucketsSource<>(
            finalKeyClass,
            sources,
            targetParallelism,
            1,
            0,
            sourceSpec,
            Metrics.distribution(getName(), getName() + "-KeyGroupSize"),
            -1,
            filenamePolicy.forTempFiles(tempDirectory),
            fileOperations,
            transformFn);

    finalizeBuckets =
        new FinalizeTransformedBuckets<>(
            fileOperations,
            newBucketMetadataFn,
            filenamePolicy.forDestination(),
            sourceSpec.hashType);
  }

  @Override
  public final WriteResult expand(PBegin begin) {
    return WriteResult.fromTuple(
        begin
            .getPipeline()
            .apply("MergeAndWriteTempBuckets", Read.from(boundedSource))
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

  public interface SerializableConsumer<ValueT> extends Consumer<ValueT>, Serializable {}

  public interface NewBucketMetadataFn<K, V> extends Serializable {
    public BucketMetadata<K, V> createMetadata(int numBuckets, int numShards, HashType hashType)
        throws CannotProvideCoderException, NonDeterministicException;
  }

  private static class FinalizeTransformedBuckets<FinalValueT>
      extends DoFn<Iterable<MergedBucket>, KV<BucketShardId, ResourceId>> {
    private final FileOperations<FinalValueT> fileOperations;
    private final NewBucketMetadataFn<?, ?> newBucketMetadataFn;
    private final FileAssignment dstFileAssignment;
    private final HashType hashType;

    static final TupleTag<KV<BucketShardId, ResourceId>> BUCKETS_TAG =
        new TupleTag<>("writtenBuckets");
    static final TupleTag<ResourceId> METADATA_TAG = new TupleTag<>("writtenMetadata");

    public FinalizeTransformedBuckets(
        FileOperations<FinalValueT> fileOperations,
        NewBucketMetadataFn<?, ?> newBucketMetadataFn,
        FileAssignment dstFileAssignment,
        HashType hashType) {
      this.fileOperations = fileOperations;
      this.newBucketMetadataFn = newBucketMetadataFn;
      this.dstFileAssignment = dstFileAssignment;
      this.hashType = hashType;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
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

  private static class MergedBucket implements Serializable {
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

  static class MergeAndWriteBucketsSource<FinalKeyT, FinalValueT>
      extends BoundedSource<MergedBucket> {
    private static final Logger LOG = LoggerFactory.getLogger(MergeAndWriteBucketsSource.class);

    private final Class<FinalKeyT> finalKeyClass;
    private final List<BucketedInput<?, ?>> sources;
    private final TargetParallelism targetParallelism;
    private final SourceSpec<FinalKeyT> sourceSpec;
    private final Distribution keyGroupSize;
    private final FileAssignment fileAssignment;
    private final FileOperations<FinalValueT> fileOperations;
    private final TransformFn<FinalKeyT, FinalValueT> transformFn;

    private final int effectiveParallelism;
    private final int bucketOffsetId;
    private long estimatedSizeBytes;

    MergeAndWriteBucketsSource(
        Class<FinalKeyT> finalKeyClass,
        List<BucketedInput<?, ?>> sources,
        TargetParallelism targetParallelism,
        int effectiveParallelism,
        int bucketOffsetId,
        SourceSpec<FinalKeyT> sourceSpec,
        Distribution keyGroupSize,
        long estimatedSizeBytes,
        FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        TransformFn<FinalKeyT, FinalValueT> transformFn) {
      this.finalKeyClass = finalKeyClass;
      this.sources = sources;
      this.targetParallelism = targetParallelism;
      this.effectiveParallelism = effectiveParallelism;
      this.bucketOffsetId = bucketOffsetId;
      this.sourceSpec = sourceSpec;
      this.estimatedSizeBytes = estimatedSizeBytes;
      this.keyGroupSize = keyGroupSize;
      this.fileAssignment = fileAssignment;
      this.fileOperations = fileOperations;
      this.transformFn = transformFn;
    }

    public MergeAndWriteBucketsSource<FinalKeyT, FinalValueT> split(
        int bucketOffsetId, int adjustedParallelism, long estimatedSizeBytes) {
      return new MergeAndWriteBucketsSource<>(
          finalKeyClass,
          sources,
          targetParallelism,
          adjustedParallelism,
          bucketOffsetId,
          sourceSpec,
          keyGroupSize,
          estimatedSizeBytes,
          fileAssignment,
          fileOperations,
          transformFn);
    }

    @Override
    public List<? extends BoundedSource<MergedBucket>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

      final int adjustedParallelism =
          SortedBucketSource.getFanout(
              sourceSpec,
              effectiveParallelism,
              targetParallelism,
              getEstimatedSizeBytes(options),
              desiredBundleSizeBytes,
              DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR);

      if (adjustedParallelism == 1) {
        return Collections.singletonList(this);
      }

      LOG.info("Parallelism was adjusted to " + adjustedParallelism);

      final long estSplitSize = estimatedSizeBytes / adjustedParallelism;

      return IntStream.range(0, adjustedParallelism)
          .boxed()
          .map(i -> this.split(i, adjustedParallelism, estSplitSize))
          .collect(Collectors.toList());
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (estimatedSizeBytes == -1) {
        estimatedSizeBytes =
            sources.parallelStream().mapToLong(BucketedInput::getOrSampleByteSize).sum();

        LOG.info("Estimated byte size is " + estimatedSizeBytes);
      }

      return estimatedSizeBytes;
    }

    @Override
    public Coder<MergedBucket> getOutputCoder() {
      return NullableCoder.of(SerializableCoder.of(MergedBucket.class));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
    }

    @Override
    public BoundedReader<MergedBucket> createReader(PipelineOptions options) throws IOException {
      final BoundedSource<MergedBucket> currentSource = this;

      return new BoundedReader<MergedBucket>() {
        private MergeBucketsReader<FinalKeyT> keyGroupReader;
        private int bucketId;
        private ResourceId dst;
        private OutputCollector<FinalValueT> outputCollector;
        private boolean started = false;

        @Override
        public boolean start() throws IOException {
          keyGroupReader =
              new MergeBucketsReader<>(
                  sources, bucketOffsetId, effectiveParallelism, sourceSpec, null, keyGroupSize);

          bucketId = bucketOffsetId;
          dst = fileAssignment.forBucket(BucketShardId.of(bucketId, 0), effectiveParallelism, 1);
          outputCollector = new OutputCollector<>(fileOperations.createWriter(dst));

          return keyGroupReader.start();
        }

        @Override
        public boolean advance() throws IOException {
          return keyGroupReader.advance();
        }

        @Override
        public MergedBucket getCurrent() throws NoSuchElementException {
          try {
            KV<FinalKeyT, CoGbkResult> mergedKeyGroup = keyGroupReader.getCurrent();
            transformFn.writeTransform(mergedKeyGroup, outputCollector);

            // Return 1 non-null value for the entire bucket
            if (!started) {
              started = true;
              return new MergedBucket(bucketId, dst, effectiveParallelism);
            } else {
              return null;
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to write merged key group", e);
          }
        }

        @Override
        public void close() throws IOException {
          outputCollector.onComplete();
        }

        @Override
        public BoundedSource<MergedBucket> getCurrentSource() {
          return currentSource;
        }
      };
    }
  }
}
