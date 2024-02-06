package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadataValue;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rehash<K, V> extends PTransform<PBegin, SortedBucketSink.WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(Rehash.class);

  private final SortedBucketSource.BucketedInput<V> bucketedInput;
  private final ResourceId outputDir;
  private final ResourceId tmpDir;
  private final HashType newHashType;
  private final String filenameSuffix;
  private final FileOperations<V> fileOperations;

  public Rehash(
      SortedBucketSource.BucketedInput<V> bucketedInput,
      ResourceId outputDir,
      ResourceId tmpDir,
      HashType newHashType,
      String filenameSuffix,
      FileOperations<V> fileOperations) {
    this.bucketedInput = bucketedInput;
    this.outputDir = outputDir;
    this.tmpDir = tmpDir;
    this.newHashType = newHashType;
    this.filenameSuffix = filenameSuffix;
    this.fileOperations = fileOperations;
  }

  @Override
  public SortedBucketSink.WriteResult expand(PBegin pBegin) {
    // Open each bucket
    final SourceMetadata<V> metadataMapping =
        BucketMetadataUtil.get().getPrimaryKeyedSourceMetadata(bucketedInput.inputs);

    assert (1 == metadataMapping.mapping.size()) : "Rehash only supports a single input partition";
    final Map.Entry<ResourceId, BucketMetadataUtil.SourceMetadataValue<V>> inputMetadata =
        metadataMapping.mapping.entrySet().iterator().next();

    final SourceMetadataValue<V> sourceMetadata = inputMetadata.getValue();
    final SMBFilenamePolicy.FileAssignment fileAssignment = sourceMetadata.fileAssignment;
    final int numBuckets = sourceMetadata.metadata.getNumBuckets();

    final SMBFilenamePolicy outputPolicy =
        new SMBFilenamePolicy(
            outputDir, sourceMetadata.metadata.getFilenamePrefix(), filenameSuffix);

    assert (null == sourceMetadata.metadata.getKeyClassSecondary())
        : "Rehash does not support secondary-keyed sources";

    // Copy metadata with new hash function
    final BucketMetadata<K, Void, V> newMetadata =
        BucketMetadata.copyWithNewHashType(
            (BucketMetadata<K, Void, V>) sourceMetadata.metadata, newHashType);

    final SMBFilenamePolicy.FileAssignment tmpFileAssignment = outputPolicy.forTempFiles(tmpDir);

    return SortedBucketSink.WriteResult.fromTuple(
        pBegin
            .apply(
                Create.of(
                    IntStream.range(0, sourceMetadata.metadata.getNumBuckets())
                        .mapToObj(
                            bucketId ->
                                KV.of(
                                    bucketId,
                                    fileAssignment.forBucket(
                                        BucketShardId.of(bucketId, 0), numBuckets, 1)))
                        .collect(Collectors.toList())))
            .apply(ParDo.of(new RehashBucket<>(tmpFileAssignment, newMetadata, fileOperations)))
            .setCoder(KvCoder.of(BucketShardId.BucketShardIdCoder.of(), ResourceIdCoder.of()))
            .apply(GroupByKey.create())
            .setCoder(
                KvCoder.of(
                    BucketShardId.BucketShardIdCoder.of(), IterableCoder.of(ResourceIdCoder.of())))
            .apply(
                ParDo.of(
                    new MergeRehashedBuckets<>(tmpFileAssignment, newMetadata, fileOperations)))
            .setCoder(KvCoder.of(BucketShardId.BucketShardIdCoder.of(), ResourceIdCoder.of()))
            .apply(
                new SortedBucketSink.RenameBuckets<>(
                    tmpFileAssignment.getDirectory(),
                    outputPolicy.forDestination(),
                    newMetadata,
                    fileOperations)));
  }

  static class RehashBucket<K, V>
      extends DoFn<KV<Integer, ResourceId>, KV<BucketShardId, ResourceId>> {
    private final SMBFilenamePolicy.FileAssignment outputFileAssignment;
    private final BucketMetadata<K, Void, V> metadata;
    private final FileOperations<V> fileOperations;

    private Map<Integer, FileOperations.Writer<V>> writers;
    private Map<Integer, ResourceId> writtenFiles;

    private final Counter recordsRead;

    RehashBucket(
        SMBFilenamePolicy.FileAssignment outputFileAssignment,
        BucketMetadata<K, Void, V> metadata,
        FileOperations<V> fileOperations) {
      this.outputFileAssignment = outputFileAssignment;
      this.metadata = metadata;
      this.fileOperations = fileOperations;
      this.writers = new HashMap<>();
      this.writtenFiles = new HashMap<>();
      this.recordsRead = Metrics.counter(Rehash.class, "RecordsRead");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final KV<Integer, ResourceId> bucketResource = context.element();
      LOG.info("Rehashing records from bucket " + bucketResource.getKey());

      fileOperations
          .iterator(bucketResource.getValue())
          .forEachRemaining(
              record -> {
                recordsRead.inc();

                final int newBucketId =
                    metadata.getBucketId(metadata.primaryComparableKeyBytes(record).primary);

                writtenFiles.computeIfAbsent(
                    newBucketId,
                    id ->
                        outputFileAssignment.forRehashedTmpBucket(
                            bucketResource.getKey(), BucketShardId.of(id, 0), metadata));

                try {
                  writers
                      .computeIfAbsent(
                          newBucketId,
                          id -> {
                            try {
                              return fileOperations.createWriter(writtenFiles.get(id));
                            } catch (IOException e) {
                              throw new RuntimeException("Failed to create writer", e);
                            }
                          })
                      .write(record);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to write bucket file", e);
                }
              });

      writtenFiles.forEach(
          (bucketId, file) -> {
            try {
              writers.get(bucketId).close();
            } catch (IOException e) {
              throw new RuntimeException("Failed to close bucket file", e);
            }
            context.output(KV.of(BucketShardId.of(bucketId, 0), file));
          });
    }
  }

  static class MergeRehashedBuckets<V>
      extends DoFn<KV<BucketShardId, Iterable<ResourceId>>, KV<BucketShardId, ResourceId>> {
    private final SMBFilenamePolicy.FileAssignment outputFileAssignment;
    private final FileOperations<V> fileOperations;
    private final BucketMetadata<?, ?, V> metadata;

    private final Counter recordsWritten;

    MergeRehashedBuckets(
        SMBFilenamePolicy.FileAssignment outputFileAssignment,
        BucketMetadata<?, ?, V> metadata,
        FileOperations<V> fileOperations) {
      this.outputFileAssignment = outputFileAssignment;
      this.fileOperations = fileOperations;
      this.metadata = metadata;
      this.recordsWritten = Metrics.counter(Rehash.class, "RecordsWritten");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final BucketShardId bucketShardId = context.element().getKey();
      final Function<V, SortedBucketIO.ComparableKeyBytes> keyFn =
          metadata::primaryComparableKeyBytes;

      final List<Iterator<KV<SortedBucketIO.ComparableKeyBytes, V>>> tmpFileIterators =
          StreamSupport.stream(context.element().getValue().spliterator(), false)
              .map(
                  tmpFileId -> {
                    try {
                      return Iterators.transform(
                          fileOperations.iterator(tmpFileId), v -> KV.of(keyFn.apply(v), v));
                    } catch (IOException e) {
                      throw new RuntimeException(
                          "Failed to create iterator for file " + tmpFileId, e);
                    }
                  })
              .collect(Collectors.toList());

      final ResourceId outputFile = outputFileAssignment.forBucket(bucketShardId, metadata);
      final FileOperations.Writer<V> writer = fileOperations.createWriter(outputFile);

      final KeyGroupIterator<V> mergingIterator =
          new KeyGroupIterator<>(tmpFileIterators, new SortedBucketIO.PrimaryKeyComparator());

      mergingIterator.forEachRemaining(
          kv ->
              kv.getValue()
                  .forEachRemaining(
                      v -> {
                        try {
                          writer.write(v);
                          recordsWritten.inc();
                        } catch (IOException e) {
                          throw new RuntimeException("Failed to write record", e);
                        }
                      }));

      writer.close();
      LOG.info("Wrote merged output file " + outputFile);

      context.output(KV.of(bucketShardId, outputFile));
    }
  }
}
