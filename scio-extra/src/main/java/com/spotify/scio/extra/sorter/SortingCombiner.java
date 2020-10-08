/*
 *   Copyright 2020 Spotify AB.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package com.spotify.scio.extra.sorter;

import com.spotify.scio.extra.sorter.SortingAccumulator.SorterCoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

public class SortingCombiner<K, V> extends CombineFn<V, SortingAccumulator, Iterable<V>> {

  private final Coder<K> sortKeyCoder;
  private final Coder<V> valueCoder;
  private final SerializableFunction<V, K> sortKeyFn;

  public static <KeyT, ValueT> SortingCombiner<KeyT, ValueT> of(
      Coder<ValueT> valueCoder,
      Coder<KeyT> sortKeyCoder,
      SerializableFunction<ValueT, KeyT> sortKeyFn) {
    return new SortingCombiner<>(valueCoder, sortKeyCoder, sortKeyFn);
  }

  private SortingCombiner(
      Coder<V> valueCoder, Coder<K> sortKeyCoder, SerializableFunction<V, K> sortKeyFn) {
    this.valueCoder = valueCoder;
    this.sortKeyCoder = sortKeyCoder;
    this.sortKeyFn = sortKeyFn;
  }

  public static <KeyT, SortingKeyT, ValueT> Combine.PerKey<KeyT, ValueT, Iterable<ValueT>> perKey(
      Coder<ValueT> valueCoder,
      Coder<SortingKeyT> sortKeyCoder,
      SerializableFunction<ValueT, SortingKeyT> sortKeyFn) {
    return Combine.perKey(SortingCombiner.of(valueCoder, sortKeyCoder, sortKeyFn));
  }

  public static <KeyT, SortingKeyT, ValueT>
      Combine.GroupedValues<KeyT, ValueT, Iterable<ValueT>> groupedValues(
          Coder<ValueT> valueCoder,
          Coder<SortingKeyT> sortKeyCoder,
          SerializableFunction<ValueT, SortingKeyT> sortKeyFn) {
    return Combine.groupedValues(SortingCombiner.of(valueCoder, sortKeyCoder, sortKeyFn));
  }

  @Override
  public Coder<SortingAccumulator> getAccumulatorCoder(
      CoderRegistry registry, Coder<V> inputCoder) {
    return SorterCoder.of();
  }

  @Override
  public SortingAccumulator createAccumulator() {
    return new SortingAccumulator();
  }

  @Override
  public SortingAccumulator addInput(SortingAccumulator mutableAccumulator, V input) {
    final K sortKey = sortKeyFn.apply(input);
    try {
      mutableAccumulator.add(
          KV.of(
              CoderUtils.encodeToByteArray(sortKeyCoder, sortKey),
              CoderUtils.encodeToByteArray(valueCoder, input)));
    } catch (CoderException e) {
      throw new RuntimeException("Caught exception encoding input", e);
    }
    return mutableAccumulator;
  }

  @Override
  public SortingAccumulator mergeAccumulators(Iterable<SortingAccumulator> accumulators) {
    return new SortingAccumulator(new MergeSortingIterable(accumulators));
  }

  @Override
  public Iterable<V> extractOutput(SortingAccumulator accumulator) {
    return () ->
        Iterators.transform(
            accumulator.sorted(false).iterator(),
            (KV<byte[], byte[]> item) -> {
              try {
                return CoderUtils.decodeFromByteArray(valueCoder, item.getValue());
              } catch (CoderException e) {
                throw new RuntimeException("Caught decoding exception", e);
              }
            });
  }

  // An iterator that merges many pre-sorted iterables together
  static class MergeSortingIterable implements Iterable<KV<byte[], byte[]>> {
    private final List<Iterator<KV<byte[], byte[]>>> sources;

    MergeSortingIterable(Iterable<SortingAccumulator> accumulators) {
      sources = new ArrayList<>();
      accumulators.forEach(
          a -> {
            sources.add(a.sorted(true).iterator());
          });
    }

    @Override
    public Iterator<KV<byte[], byte[]>> iterator() {
      return Iterators.mergeSorted(sources, SortingAccumulator.comparator);
    }
  }
}
