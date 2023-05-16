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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.ComparableKeyBytes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;

// FIXME: current limitation: must exhaust Iterator<ValueT> before starting the next key group
class KeyGroupIterator<V> implements Iterator<KV<ComparableKeyBytes, Iterator<V>>> {
  private final List<PeekingIterator<KV<ComparableKeyBytes, V>>> iterators;
  final Comparator<ComparableKeyBytes> keyComparator;

  private Iterator<V> currentGroup = null;

  KeyGroupIterator(
      List<Iterator<KV<ComparableKeyBytes, V>>> iterators,
      Comparator<ComparableKeyBytes> keyComparator) {
    this.iterators =
        iterators.stream().map(Iterators::peekingIterator).collect(Collectors.toList());
    this.keyComparator = keyComparator;
  }

  @Override
  public boolean hasNext() {
    checkState();
    return iterators.stream().anyMatch(PeekingIterator::hasNext);
  }

  private ComparableKeyBytes min() {
    return iterators.stream()
        .filter(Iterator::hasNext)
        .map(it -> it.peek().getKey())
        .min(keyComparator)
        .get();
  }

  @Override
  public KV<ComparableKeyBytes, Iterator<V>> next() {
    checkState();
    ComparableKeyBytes k = min();

    /* The key `k` is fixed for all usages of this iterator. Here we iterate over
     * all shards, and if we find the min key at the head of a shard, there is
     * another `V` value to be consumed. Otherwise, we increment `currentIteratorIdx`
     * to move on to the next shard, eventually having checked all shards for the
     * minimum key, at which point this iterator can be garbage collected.
     */
    Iterator<V> vi =
        new Iterator<V>() {
          private int currentIterator = 0;

          @Override
          public boolean hasNext() {
            PeekingIterator<KV<ComparableKeyBytes, V>> currentIt;
            while (currentIterator < iterators.size()) {
              currentIt = iterators.get(currentIterator);
              if (currentIt.hasNext()) {
                ComparableKeyBytes nextK = currentIt.peek().getKey();
                if (keyComparator.compare(k, nextK) == 0) {
                  return true;
                }
              }
              currentIterator++;
            }
            currentGroup = null;
            return false;
          }

          @Override
          public V next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return iterators.get(currentIterator).next().getValue();
          }
        };

    currentGroup = vi;
    return KV.of(k, vi);
  }

  private void checkState() {
    Preconditions.checkState(currentGroup == null, "Previous Iterator<ValueT> not fully iterated");
  }
}
