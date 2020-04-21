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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
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
 * BucketMetadata#getKeyBytes(Object)}, and assign it to an Integer bucket using {@link
 * BucketMetadata#getBucketId(byte[])}. Next, a {@link GroupByKey} transform is applied to create a
 * {@link PCollection} of {@code N} elements, where {@code N} is the number of buckets specified by
 * {@link BucketMetadata#getNumBuckets()}, then a {@link SortValues} transform is used to sort
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
 * memory. Note that the {@link SortValues} transform will try to sort in-memory and fall back to an
 * {@link ExternalSorter} if needed.
 *
 * <p>Each bucket can be further sharded to reduce the impact of hot keys, by specifying {@link
 * BucketMetadata#getNumShards()}.
 *
 * @param <K> the type of the keys that values in a bucket are sorted with
 * @param <V> the type of the values in a bucket
 */
public class SortedBucketSink<K, V> extends PTransform<PCollection<V>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSink.class);

  private final BucketMetadata<K, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<V> fileOperations;
  private final int sorterMemoryMb;

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
      int sorterMemoryMb) {
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy = new SMBFilenamePolicy(outputDirectory, filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.sorterMemoryMb = sorterMemoryMb;
  }

  @Override
  public final WriteResult expand(PCollection<V> input) {
    // @Todo: should we allow windowed writes?
    Preconditions.checkArgument(
        input.isBounded() == IsBounded.BOUNDED,
        "SortedBucketSink cannot be applied to a non-bounded PCollection");
    final Coder<V> valueCoder = input.getCoder();
    return input
        .apply("ExtractKeys", ParDo.of(new ExtractKeys<>(this.bucketMetadata, valueCoder)))
        .setCoder(
            KvCoder.of(
                BucketShardIdCoder.of(), KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of())))
        .apply("GroupByKey", GroupByKey.create())
        .apply(
            "SortValues",
            ParDo.of(
                new SortBytesDoFn<>(
                    getName(),
                    BufferedExternalSorter.options()
                        .withExternalSorterType(ExternalSorter.Options.SorterType.NATIVE)
                        .withMemoryMB(sorterMemoryMb))))
        .apply(
            "WriteOperation",
            new WriteOperation<>(
                filenamePolicy, bucketMetadata, fileOperations, tempDirectory, valueCoder));
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  private static class ExtractKeys<K, V> extends DoFn<V, KV<BucketShardId, KV<byte[], byte[]>>> {
    // Substitute null keys in the output KV<byte[], V> so that they survive serialization
    private static final byte[] NULL_SORT_KEY = new byte[0];

    ExtractKeys(BucketMetadata<K, V> bucketMetadata, Coder<V> valueCoder) {
      this.bucketMetadata = bucketMetadata;
      this.valueCoder = valueCoder;
    }

    private final BucketMetadata<K, V> bucketMetadata;
    private final Coder<V> valueCoder;
    private transient int shardId;

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

    @ProcessElement
    public void processElement(ProcessContext c) {
      final V record = c.element();
      final byte[] keyBytes = bucketMetadata.getKeyBytes(record);

      final BucketShardId bucketShardId =
          keyBytes != null
              ? BucketShardId.of(bucketMetadata.getBucketId(keyBytes), shardId)
              : BucketShardId.ofNullKey(shardId);

      final byte[] sortKey = keyBytes != null ? keyBytes : NULL_SORT_KEY;
      try {
        final byte[] valueBytes = CoderUtils.encodeToByteArray(valueCoder, record);
        c.output(KV.of(bucketShardId, KV.of(sortKey, valueBytes)));
      } catch (CoderException e) {
        throw new RuntimeException("Caught coder exception: " + e);
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
    }
  }

  /**
   * Patch of {@link SortValues}'s SortValuesDoFn which acts on elements already encoded as bytes
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
  // TODO: Retry policy, etc...
  static class WriteOperation<V>
      extends PTransform<
          PCollection<KV<BucketShardId, Iterable<KV<byte[], byte[]>>>>, WriteResult> {
    private final SMBFilenamePolicy filenamePolicy;
    private final BucketMetadata<?, V> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final ResourceId tempDirectory;
    private final Coder<V> valueCoder;

    WriteOperation(
        SMBFilenamePolicy filenamePolicy,
        BucketMetadata<?, V> bucketMetadata,
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
      return input
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
              new FinalizeTempFiles<>(
                  filenamePolicy.forDestination(), bucketMetadata, fileOperations));
    }
  }

  /** Writes metadata and bucket files to temporary location. */
  static class WriteTempFiles<V>
      extends DoFn<KV<BucketShardId, Iterable<KV<byte[], byte[]>>>, KV<BucketShardId, ResourceId>> {

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
                throw new RuntimeException("Failed to write sorted-bucket file", e);
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

  /** Moves temporary files to final destinations. */
  static class FinalizeTempFiles<V>
      extends PTransform<PCollection<KV<BucketShardId, ResourceId>>, WriteResult> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;

    FinalizeTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        FileOperations<V> fileOperations) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
    }

    @Override
    public WriteResult expand(PCollection<KV<BucketShardId, ResourceId>> input) {
      final PCollectionTuple output =
          input.apply(
              "MoveToFinalDestinations",
              new RenameBuckets<>(fileAssignment, bucketMetadata, fileOperations));

      return new WriteResult(
          input.getPipeline(),
          output.get(new TupleTag<ResourceId>("writtenMetadata")).setCoder(ResourceIdCoder.of()),
          output
              .get(new TupleTag<KV<BucketShardId, ResourceId>>("writtenBuckets"))
              .setCoder(KvCoder.of(BucketShardIdCoder.of(), ResourceIdCoder.of())));
    }

    /** Renames temp bucket files to final destinations. */
    private static class RenameBuckets<V>
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
            input.apply("WrittenBucketsShards", View.asMap());

        List<BucketShardId> allBucketsAndShards = new ArrayList<>();
        for (int i = 0; i < bucketMetadata.getNumBuckets(); i++) {
          for (int j = 0; j < bucketMetadata.getNumShards(); j++) {
            allBucketsAndShards.add(BucketShardId.of(i, j));
          }
        }
        final TupleTag<KV<BucketShardId, ResourceId>> bucketsTag = new TupleTag<>("writtenBuckets");
        final TupleTag<ResourceId> metadataTag = new TupleTag<>("writtenMetadata");
        final Values<BucketShardId> createBuckets =
            Create.of(allBucketsAndShards).withCoder(BucketShardIdCoder.of());
        return input
            .getPipeline()
            .apply("EmptyBucketsShards", createBuckets)
            .apply("GroupAll", Group.globally())
            .apply(
                "PopulateFinalDst",
                ParDo.of(
                        new DoFn<Iterable<BucketShardId>, KV<BucketShardId, ResourceId>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            // Write metadata file first
                            final ResourceId metadataDst = writeMetadataFile();

                            final List<ResourceId> filesToCleanUp = new ArrayList<>();
                            filesToCleanUp.add(metadataDst);

                            // Transfer bucket files once metadata has been written
                            final List<ResourceId> srcFiles = new ArrayList<>();
                            final List<ResourceId> dstFiles = new ArrayList<>();

                            final List<KV<BucketShardId, ResourceId>> finalBucketDsts =
                                new ArrayList<>();

                            final Map<BucketShardId, ResourceId> side = c.sideInput(writtenBuckets);

                            for (BucketShardId id : c.element()) {
                              final ResourceId tmpDst = side.get(id);
                              final ResourceId finalDst =
                                  fileAssignment.forBucket(id, bucketMetadata);

                              // If bucket hasn't been written, write empty file
                              if (tmpDst == null) {
                                try {
                                  fileOperations.createWriter(finalDst).close();
                                  filesToCleanUp.add(finalDst);
                                } catch (IOException e) {
                                  cleanupTempFiles(e, filesToCleanUp);
                                  throw new RuntimeException("Failed to write empty file", e);
                                }

                                c.output(bucketsTag, KV.of(id, finalDst));
                              } else {
                                srcFiles.add(tmpDst);
                                dstFiles.add(finalDst);
                                finalBucketDsts.add(KV.of(id, finalDst));
                              }
                            }

                            LOG.info("Renaming bucket files");
                            try {
                              FileSystems.rename(srcFiles, dstFiles);
                            } catch (IOException e) {
                              cleanupTempFiles(e, filesToCleanUp);
                              throw new RuntimeException("Failed to rename temporary files", e);
                            }

                            finalBucketDsts.forEach(kv -> c.output(bucketsTag, kv));
                            c.output(metadataTag, metadataDst);
                          }
                        })
                    .withSideInputs(writtenBuckets)
                    .withOutputTags(bucketsTag, TupleTagList.of(metadataTag)));
      }

      @SuppressWarnings("unchecked")
      private ResourceId writeMetadataFile() {
        final ResourceId dst = fileAssignment.forMetadata();
        LOG.info("Writing metadata to file {}", dst);

        try (final OutputStream outputStream =
            Channels.newOutputStream(FileSystems.create(dst, "application/json"))) {
          BucketMetadata.to(bucketMetadata, outputStream);
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
  }

  private static void cleanupTempFiles(Exception cause, Collection<ResourceId> files) {
    LOG.info(
        "Deleting temporary file {}",
        files.stream().map(ResourceId::toString).collect(Collectors.joining(", ")));
    try {
      FileSystems.delete(files, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    } catch (IOException e) {
      cause.addSuppressed(e);
    }
  }
}
