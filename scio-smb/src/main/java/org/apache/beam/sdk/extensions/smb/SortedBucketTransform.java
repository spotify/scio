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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.SourceSpec;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

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
    final SourceSpec<FinalKeyT> sourceSpec = SortedBucketSource.getSourceSpec(finalKeyClass, sources);

    Preconditions.checkArgument(
        bucketMetadata.getNumBuckets() == sourceSpec.leastNumBuckets,
        "Specified number of buckets %s does not match smallest bucket size among"
            + " inputs: %s.", bucketMetadata.getNumBuckets(), sourceSpec.leastNumBuckets
    );

    final FileAssignment tempFileAssignment = filenamePolicy.forTempFiles(tempDirectory);

    final Create.Values<Integer> createBuckets = Create.of(
        IntStream.range(0, sourceSpec.leastNumBuckets).boxed().collect(Collectors.toList())
    ).withCoder(VarIntCoder.of());

    final Create.Values<ResourceId> writeTempMetadata =
        SortedBucketSink.WriteTempFiles.writeMetadataTransform(tempFileAssignment, bucketMetadata);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<Integer> reshuffle = Reshuffle.viaRandomKey();

    return PCollectionTuple
        .of(
            new TupleTag<>("TempMetadata"),
            begin
                .getPipeline()
                .apply("WriteTempMetadata", writeTempMetadata)
                .setCoder(ResourceIdCoder.of())
        ).and(
            new TupleTag<>("TempBuckets"),
            begin.getPipeline()
                .apply("CreateBuckets", createBuckets)
                .apply("ReshuffleKeys", reshuffle)
                .apply(
                    "MergeTransformWrite",
                    ParDo.of(new MergeAndWriteBuckets<>(
                      sources,
                      sourceSpec.keyCoder,
                      tempFileAssignment,
                      fileOperations,
                      bucketMetadata,
                      transformFn)
                  )
                ).setCoder(KvCoder.of(BucketShardIdCoder.of(), ResourceIdCoder.of()))
        ).apply(
            "FinalizeTempFiles",
            new SortedBucketSink.FinalizeTempFiles<>(
                filenamePolicy.forDestination(), bucketMetadata, fileOperations)
        );
  }

  @FunctionalInterface
  public interface TransformFn<KeyT, ValueT> extends Serializable {
    void writeTransform(KV<KeyT, CoGbkResult> keyGroup, OutputCollector<ValueT> outputConsumer);
  }

  public static class OutputCollector<ValueT> implements Consumer<ValueT>, Serializable {
    private final Writer<ValueT> writer;

    public OutputCollector(Writer<ValueT> writer) {
      this.writer = writer;
    }

    public void onComplete() {
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

  private static class MergeAndWriteBuckets<FinalKeyT, FinalValueT> extends DoFn<Integer, KV<BucketShardId, ResourceId>> {
    private final List<BucketedInput<?, ?>> sources;
    private final FileAssignment fileAssignment;
    private final FileOperations<FinalValueT> fileOperations;
    private final TransformFn<FinalKeyT, FinalValueT> transformFn;
    private final Coder<FinalKeyT> keyCoder;
    private final int numBuckets;
    private final int numShards;

    MergeAndWriteBuckets(
        List<BucketedInput<?, ?>> sources,
        Coder<FinalKeyT> keyCoder,
        FileAssignment fileAssignment,
        FileOperations<FinalValueT> fileOperations,
        BucketMetadata<FinalKeyT, FinalValueT> bucketMetadata,
        TransformFn<FinalKeyT, FinalValueT> transformFn
    ) {
      this.keyCoder = keyCoder;
      this.sources = sources;
      this.fileAssignment = fileAssignment;
      this.fileOperations = fileOperations;
      this.transformFn = transformFn;
      this.numBuckets = bucketMetadata.getNumBuckets();
      this.numShards = bucketMetadata.getNumShards();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final int bucketId = c.element();
      final int numSources = sources.size();

      @SuppressWarnings("unchecked")
      final OutputCollector<FinalValueT>[] writers = new OutputCollector[numShards];
      final List<KV<BucketShardId, ResourceId>> bucketShardsToDsts = new ArrayList<>();

      for (int shardId = 0; shardId < numShards; shardId++) {
        final BucketShardId bucketShardId = BucketShardId.of(bucketId, shardId);
        final ResourceId dst = fileAssignment.forBucket(bucketShardId, numBuckets, numShards);

        try {
          writers[shardId] = new OutputCollector<>(fileOperations.createWriter(dst));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        bucketShardsToDsts.add(KV.of(bucketShardId, dst));
      }

      final KeyGroupIterator[] iterators = sources.stream()
          .map(i -> i.createIterator(bucketId, numBuckets))
          .toArray(KeyGroupIterator[]::new);

      // Supplies sharded writers per key group in round-robin style
      final Supplier<OutputCollector<FinalValueT>> writerSupplier;
      if (numShards == 1) {
        writerSupplier = () -> writers[0];
      } else {
        writerSupplier = new Supplier<OutputCollector<FinalValueT>>() {
          private int shard = 0;

          @Override
          public OutputCollector<FinalValueT> get() {
            final OutputCollector<FinalValueT> result = writers[shard];
            shard = (shard + 1) % numShards;
            return result;
          }
        };
      }

      final Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();
      final CoGbkResultSchema resultSchema = BucketedInput.schemaOf(sources);
      final TupleTagList tupleTags = resultSchema.getTupleTagList();

      while (true) {
        int completedSources = 0;
        for (int i = 0; i < numSources; i++) {
          final KeyGroupIterator it = iterators[i];
          if (nextKeyGroups.containsKey(tupleTags.get(i))) {
            continue;
          }
          if (it.hasNext()) {
            @SuppressWarnings("unchecked") final KV<byte[], Iterator<?>> next = it.next();
            nextKeyGroups.put(tupleTags.get(i), next);
          } else {
            completedSources++;
          }
        }

        if (nextKeyGroups.isEmpty()) {
          break;
        }

        transformFn.writeTransform(
            SortedBucketSource.MergeBuckets.mergeKeyGroup(nextKeyGroups, resultSchema, keyCoder),
            writerSupplier.get()
        );

        if (completedSources == numSources) {
          break;
        }
      }

      bucketShardsToDsts.forEach(bucketShardAndDst -> {
        writers[bucketShardAndDst.getKey().getShardId()].onComplete();
        c.output(bucketShardAndDst);
      });
    }
  }
}
