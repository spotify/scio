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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;

/**
 * A {@link PTransform} that encapsulates both a {@link SortedBucketSource} and {@link
 * SortedBucketSink} operation, with a user-supplied transform function mapping merged {@link
 * CoGbkResult}s to their final writable outputs. The same hash function must be supplied in the
 * output {@link BucketMetadata} to preserve the same key distribution.
 *
 * @param <FinalKeyT>
 * @param <FinalValueT>
 */
public class SortedBucketTransform<FinalKeyT, FinalValueT> extends PTransform<PBegin, WriteResult> {
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<FinalValueT> fileOperations;
  private final Class<FinalKeyT> finalKeyClass;
  private final List<BucketedInput<?, ?>> sources;
  private final BucketMetadata<FinalKeyT, FinalValueT> bucketMetadata;
  private final TransformFn<FinalKeyT, FinalValueT> transformFn;

  public SortedBucketTransform(
      Class<FinalKeyT> finalKeyClass,
      BucketMetadata<FinalKeyT, FinalValueT> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<FinalValueT> fileOperations,
      List<BucketedInput<?, ?>> sources,
      TransformFn<FinalKeyT, FinalValueT> transformFn) {
    this.filenamePolicy = new SMBFilenamePolicy(outputDirectory, filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.finalKeyClass = finalKeyClass;
    this.sources = sources;
    this.transformFn = transformFn;
    this.bucketMetadata = bucketMetadata;
  }

  @Override
  public final WriteResult expand(PBegin begin) {
    Preconditions.checkArgument(
        bucketMetadata.getNumShards() == 1,
        "Sharding is not supported in SortedBucketTransform. numShards must == 1.");

    final SourceSpec<FinalKeyT> sourceSpec = SourceSpec.from(finalKeyClass, sources);

    Preconditions.checkArgument(
        bucketMetadata.getNumBuckets() >= sourceSpec.leastNumBuckets,
        "numBuckets in BucketMetadata must be >= leastNumBuckets among sources: "
            + sourceSpec.leastNumBuckets);

    final FileAssignment tempFileAssignment = filenamePolicy.forTempFiles(tempDirectory);

    final Create.Values<Integer> createBuckets =
        Create.of(
                IntStream.range(0, sourceSpec.leastNumBuckets).boxed().collect(Collectors.toList()))
            .withCoder(VarIntCoder.of());

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<Integer> reshuffle = Reshuffle.viaRandomKey();

    return WriteResult.fromTuple(
        begin
            .getPipeline()
            .apply("CreateBuckets", createBuckets)
            .apply("ReshuffleKeys", reshuffle)
            .apply(
                "MergeTransformWrite",
                ParDo.of(
                    new MergeAndWriteBuckets<>(
                        this.getName(),
                        sources,
                        sourceSpec,
                        tempFileAssignment,
                        fileOperations,
                        bucketMetadata,
                        transformFn)))
            .setCoder(KvCoder.of(BucketShardIdCoder.of(), ResourceIdCoder.of()))
            .apply(
                "FinalizeTempFiles",
                new SortedBucketSink.RenameBuckets<>(
                    filenamePolicy.forDestination(), bucketMetadata, fileOperations)));
  }

  @FunctionalInterface
  public interface TransformFn<KeyT, ValueT> extends Serializable {
    void writeTransform(
        KV<KeyT, CoGbkResult> keyGroup, SerializableConsumer<ValueT> outputConsumer);
  }

  public interface SerializableConsumer<ValueT> extends Consumer<ValueT>, Serializable {}

  private static class OutputCollector<ValueT> implements SerializableConsumer<ValueT> {
    private final Writer<ValueT> writer;
    private final Counter elementsWritten;

    OutputCollector(Writer<ValueT> writer, Counter elementsWritten) {
      this.writer = writer;
      this.elementsWritten = elementsWritten;
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
        elementsWritten.inc();
      } catch (IOException e) {
        throw new RuntimeException("Write of element " + t + " failed: ", e);
      }
    }
  }

  private static class MergeAndWriteBuckets<FinalKeyT, FinalValueT>
      extends DoFn<Integer, KV<BucketShardId, ResourceId>> {
    private static final Comparator<byte[]> bytesComparator =
        UnsignedBytes.lexicographicalComparator();

    private static final Comparator<Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final List<BucketedInput<?, ?>> sources;
    private final FileAssignment fileAssignment;
    private final FileOperations<FinalValueT> fileOperations;
    private final TransformFn<FinalKeyT, FinalValueT> transformFn;
    private final BucketMetadata<FinalKeyT, FinalValueT> bucketMetadata;
    private final Coder<FinalKeyT> keyCoder;
    private final int leastNumBuckets;

    private final Counter elementsWritten;
    private final Counter elementsRead;
    private final Distribution keyGroupSize;

    MergeAndWriteBuckets(
        String transformName,
        List<BucketedInput<?, ?>> sources,
        SourceSpec<FinalKeyT> sourceSpec,
        FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        BucketMetadata<FinalKeyT, FinalValueT> bucketMetadata,
        TransformFn<FinalKeyT, FinalValueT> transformFn) {
      this.sources = sources;
      this.fileAssignment = fileAssignment;
      this.fileOperations = fileOperations;
      this.transformFn = transformFn;
      this.bucketMetadata = bucketMetadata;
      this.keyCoder = sourceSpec.keyCoder;
      this.leastNumBuckets = sourceSpec.leastNumBuckets;

      elementsWritten =
          Metrics.counter(SortedBucketTransform.class, transformName + "-ElementsWritten");
      elementsRead = Metrics.counter(SortedBucketTransform.class, transformName + "-ElementsRead");
      keyGroupSize =
          Metrics.distribution(SortedBucketTransform.class, transformName + "-KeyGroupSize");
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("keyCoder", keyCoder.getClass()));
      builder.add(DisplayData.item("numBuckets", bucketMetadata.getNumBuckets()));
      builder.add(DisplayData.item("numShards", bucketMetadata.getNumShards()));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final int bucketId = c.element();
      final boolean reHashBucket = bucketMetadata.getNumBuckets() > leastNumBuckets;

      final Map<Integer, OutputCollector<FinalValueT>> bucketsToWriters = new HashMap<>();
      final List<KV<BucketShardId, ResourceId>> bucketsToDsts = new ArrayList<>();

      for (int bucketFanout = bucketId;
          bucketFanout < bucketMetadata.getNumBuckets();
          bucketFanout += leastNumBuckets) {
        final BucketShardId bucketShardId = BucketShardId.of(bucketFanout, 0);
        final ResourceId dst = fileAssignment.forBucket(bucketShardId, bucketMetadata);

        try {
          bucketsToWriters.put(
              bucketFanout,
              new OutputCollector<>(fileOperations.createWriter(dst), elementsWritten));
          bucketsToDsts.add(KV.of(bucketShardId, dst));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      final KeyGroupIterator[] iterators =
          sources.stream()
              .map(i -> i.createIterator(c.element(), leastNumBuckets))
              .toArray(KeyGroupIterator[]::new);

      merge(
          iterators,
          BucketedInput.schemaOf(sources),
          (bytes) -> true,
          mergedKeyGroup -> {
            int assignedBucket =
                reHashBucket ? bucketMetadata.getBucketId(mergedKeyGroup.getKey()) : bucketId;

            try {
              transformFn.writeTransform(
                  KV.of(
                      keyCoder.decode(new ByteArrayInputStream(mergedKeyGroup.getKey())),
                      mergedKeyGroup.getValue()),
                  bucketsToWriters.get(assignedBucket));
            } catch (Exception e) {
              throw new RuntimeException("Failed to decode and merge key group", e);
            }
          },
          elementsRead,
          keyGroupSize);

      bucketsToDsts.forEach(
          bucketShardAndDst -> {
            bucketsToWriters.get(bucketShardAndDst.getKey().getBucketId()).onComplete();
            c.output(bucketShardAndDst);
          });
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
              @SuppressWarnings("unchecked")
              final List<Object> values = (List<Object>) valueMap.get(index);
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
  }
}
