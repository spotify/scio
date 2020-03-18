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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;

// FIXME: current limitation: must exhaust Iterator<ValueT> before starting the next key group
class KeyGroupIterator<KeyT, ValueT> implements Iterator<KV<KeyT, Iterator<ValueT>>> {
  private final List<PeekingIterator<ValueT>> iterators;
  private final Function<ValueT, KeyT> keyFn;
  private final Comparator<KeyT> keyComparator;

  private Iterator<ValueT> currentGroup = null;

  // FIXME: remove
  KeyGroupIterator(
      Iterator<ValueT> iterator, Function<ValueT, KeyT> keyFn, Comparator<KeyT> keyComparator) {
    this.iterators = Collections.singletonList(Iterators.peekingIterator(iterator));
    this.keyFn = keyFn;
    this.keyComparator = keyComparator;
  }

  KeyGroupIterator(
      List<Iterator<ValueT>> iterators,
      Function<ValueT, KeyT> keyFn,
      Comparator<KeyT> keyComparator) {
    this.iterators =
        iterators.stream().map(Iterators::peekingIterator).collect(Collectors.toList());
    this.keyFn = keyFn;
    this.keyComparator = keyComparator;
  }

  @Override
  public boolean hasNext() {
    checkState();
    return iterators.stream().anyMatch(it -> it.hasNext() && it.peek() != null);
  }

  private KeyT min() {
    return iterators.stream()
        .filter(Iterator::hasNext)
        .map(it -> keyFn.apply(it.peek()))
        .min(keyComparator)
        .get();
  }

  @Override
  public KV<KeyT, Iterator<ValueT>> next() {
    checkState();
    KeyT k = min();

    Iterator<ValueT> vi =
        new Iterator<ValueT>() {
          private int currentIterator = 0;

          @Override
          public boolean hasNext() {
            PeekingIterator<ValueT> currentIt;
            while (currentIterator < iterators.size()) {
              currentIt = iterators.get(currentIterator);
              if (currentIt.hasNext()) {
                KeyT nextK = keyFn.apply(currentIt.peek());
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
          public ValueT next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return iterators.get(currentIterator).next();
          }
        };

    currentGroup = vi;
    return KV.of(k, vi);
  }

  private void checkState() {
    Preconditions.checkState(currentGroup == null, "Previous Iterator<ValueT> not fully iterated");
  }
}
