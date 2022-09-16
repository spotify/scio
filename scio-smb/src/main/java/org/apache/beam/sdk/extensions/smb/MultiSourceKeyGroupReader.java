/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;

@SuppressWarnings("unchecked")
public class MultiSourceKeyGroupReader<FinalKeyT> {
  private enum AcceptKeyGroup {
    ACCEPT,
    REJECT,
    UNSET
  }

  private enum KeyGroupOutputSize {
    EMPTY,
    NONEMPTY
  }

  private KV<FinalKeyT, CoGbkResult> head = null;
  private boolean initialized = false;

  private final Coder<FinalKeyT> keyCoder;

  private int runningKeyGroupSize = 0;
  private final Distribution keyGroupSize;
  private final boolean materializeKeyGroup;
  private final Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

  private final CoGbkResultSchema resultSchema;
  private final List<BucketIterator<?, ?>> bucketedInputs;
  private final Function<byte[], Boolean> keyGroupFilter;

  public MultiSourceKeyGroupReader(
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
      SourceSpec<FinalKeyT> sourceSpec,
      Distribution keyGroupSize,
      boolean materializeKeyGroup,
      int bucketId,
      int effectiveParallelism,
      PipelineOptions options) {
    this.keyCoder = sourceSpec.keyCoder;
    this.keyGroupSize = keyGroupSize;
    this.materializeKeyGroup = materializeKeyGroup;

    // source TupleTags `and`-ed together
    this.resultSchema = SortedBucketSource.BucketedInput.schemaOf(sources);
    this.bucketedInputs =
        sources.stream()
            .map(src -> new BucketIterator<>(src, bucketId, effectiveParallelism, options))
            .collect(Collectors.toList());
    this.keyGroupFilter =
        (bytes) ->
            sources.get(0).getMetadata().rehashBucket(bytes, effectiveParallelism) == bucketId;
  }

  public KV<FinalKeyT, CoGbkResult> readNext() {
    advance();
    return head;
  }

  private void advance() {
    // once all sources are exhausted, head is empty, so short circuit return
    if (initialized && head == null) return;

    initialized = true;
    while (true) {
      if (runningKeyGroupSize != 0) {
        keyGroupSize.update(runningKeyGroupSize);
        runningKeyGroupSize = 0;
      }

      // advance iterators whose values have already been used.
      bucketedInputs.stream()
          .filter(BucketIterator::shouldAdvance)
          .forEach(BucketIterator::advance);

      // only operate on the non-exhausted sources
      List<BucketIterator<?, ?>> activeSources =
          bucketedInputs.stream().filter(BucketIterator::notExhausted).collect(Collectors.toList());

      // once all sources are exhausted, set head to empty and return
      if (activeSources.isEmpty()) {
        head = null;
        break;
      }

      // process keys in order, but since not all sources
      // have all keys, find the minimum available key
      final List<byte[]> consideredKeys =
          activeSources.stream().map(BucketIterator::currentKey).collect(Collectors.toList());
      byte[] minKey = consideredKeys.stream().min(bytesComparator).orElse(null);
      final boolean emitBasedOnMinKeyBucketing = keyGroupFilter.apply(minKey);

      // output accumulator
      final List<Iterable<?>> valueMap =
          IntStream.range(0, resultSchema.size())
              .mapToObj(i -> new ArrayList<>())
              .collect(Collectors.toList());

      // When a predicate is applied, a source containing a key may have no values after filtering.
      // Sources containing minKey are by default known to be NONEMPTY. Once all sources are
      // consumed, if all are known to be empty, the key group can be dropped.
      List<KeyGroupOutputSize> valueOutputSizes =
          IntStream.range(0, resultSchema.size())
              .mapToObj(i -> KeyGroupOutputSize.EMPTY)
              .collect(Collectors.toList());

      // minKey will be accepted or rejected by the first source which has it.
      // acceptKeyGroup short-circuits the 'emit' logic below once a decision is made on minKey.
      AcceptKeyGroup acceptKeyGroup = AcceptKeyGroup.UNSET;
      for (BucketIterator<?, ?> src : activeSources) {
        // for each source, if the current key is equal to the minimum, consume it
        if (src.notExhausted() && bytesComparator.compare(minKey, src.currentKey()) == 0) {
          // if this key group has been previously accepted by a preceding source, emit.
          // "  "    "   "     "   "    "          rejected by a preceding source, don't emit.
          // if this is the first source for this key group, emit if either the source settings or
          // the min key say we should.
          boolean emitKeyGroup =
              (acceptKeyGroup == AcceptKeyGroup.ACCEPT)
                  || ((acceptKeyGroup == AcceptKeyGroup.UNSET)
                      && (src.emitByDefault || emitBasedOnMinKeyBucketing));

          final Iterator<Object> keyGroupIterator = (Iterator<Object>) src.currentValue();
          if (emitKeyGroup) {
            acceptKeyGroup = AcceptKeyGroup.ACCEPT;
            // data must be eagerly materialized if requested or if there is a predicate
            boolean materialize = materializeKeyGroup || (src.predicate != null);
            int outputIndex = resultSchema.getIndex(src.tupleTag);

            if (!materialize) {
              // this source contains minKey, so is known to contain at least one value
              valueOutputSizes.set(outputIndex, KeyGroupOutputSize.NONEMPTY);
              // lazy data iterator
              valueMap.set(
                  outputIndex,
                  new SortedBucketSource.TraversableOnceIterable<>(
                      Iterators.transform(
                          keyGroupIterator,
                          (value) -> {
                            runningKeyGroupSize++;
                            return value;
                          })));

            } else {
              // eagerly materialize this iterator and apply the predicate to each value
              // this must be eager because the predicate can operate on the entire collection
              final List<Object> values = (List<Object>) valueMap.get(outputIndex);
              final SortedBucketSource.Predicate<Object> predicate =
                  (src.predicate == null)
                      ? ((xs, x) -> true)
                      : (SortedBucketSource.Predicate<Object>) src.predicate;
              keyGroupIterator.forEachRemaining(
                  v -> {
                    if ((predicate).apply(values, v)) {
                      values.add(v);
                      runningKeyGroupSize++;
                    }
                  });
              KeyGroupOutputSize sz =
                  values.isEmpty() ? KeyGroupOutputSize.EMPTY : KeyGroupOutputSize.NONEMPTY;
              valueOutputSizes.set(outputIndex, sz);
            }
          } else {
            acceptKeyGroup = AcceptKeyGroup.REJECT;
            // skip key but still have to exhaust iterator
            keyGroupIterator.forEachRemaining(value -> {});
          }
        }
      }

      if (acceptKeyGroup == AcceptKeyGroup.ACCEPT) {
        // if all outputs are known-empty, omit this key group
        boolean allEmpty = valueOutputSizes.stream().allMatch(s -> s == KeyGroupOutputSize.EMPTY);
        if (!allEmpty) {
          final KV<byte[], CoGbkResult> next =
              KV.of(minKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
          try {
            // new head found, we're done
            head = KV.of(keyCoder.decode(new ByteArrayInputStream(next.getKey())), next.getValue());
            break;
          } catch (Exception e) {
            throw new RuntimeException("Failed to decode key group", e);
          }
        }
      }
    }
  }

  private static class BucketIterator<K, V> {
    public final TupleTag<?> tupleTag;
    public final boolean emitByDefault;

    private final KeyGroupIterator<byte[], V> iter;
    final SortedBucketSource.Predicate<V> predicate;
    private KV<byte[], Iterator<V>> head;

    BucketIterator(
        SortedBucketSource.BucketedInput<K, V> source,
        int bucketId,
        int parallelism,
        PipelineOptions options) {
      this.predicate = source.getPredicate();
      this.tupleTag = source.getTupleTag();
      this.iter = source.createIterator(bucketId, parallelism, options);

      int numBuckets = source.getMetadata().getNumBuckets();
      // The canonical # buckets for this source. If # buckets >= the parallelism of the job,
      // we know that the key doesn't need to be re-hashed as it's already in the right bucket.
      this.emitByDefault = numBuckets >= parallelism;

      advance();
    }

    public byte[] currentKey() {
      return head.getKey();
    }

    public Iterator<V> currentValue() {
      return head.getValue();
    }

    public boolean notExhausted() {
      return head != null;
    }

    public boolean shouldAdvance() {
      // should advance if current values have been consumed and there's more data available
      return notExhausted() && (!currentValue().hasNext());
    }

    public void advance() {
      if (iter.hasNext()) {
        head = iter.next();
      } else {
        head = null;
      }
    }
  }
}
