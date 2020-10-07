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

import com.google.common.primitives.UnsignedBytes;
import com.spotify.scio.extra.sorter.SortingCombiner.MergeSortingIterable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;

public class SortingAccumulator {
  private boolean isSorted;
  private boolean isEmpty;
  private Iterable<KV<byte[], byte[]>> items;

  private static final Comparator<KV<byte[], byte[]>> comparator =
      (kv1, kv2) -> UnsignedBytes.lexicographicalComparator().compare(kv1.getKey(), kv2.getKey());

  public SortingAccumulator() {
    this.isSorted = false;
    this.isEmpty = true;
  }

  SortingAccumulator(boolean isEmpty, Iterable<KV<byte[], byte[]>> sortedItems, boolean isSorted) {
    this.isEmpty = isEmpty;
    this.items = sortedItems;
    this.isSorted = isSorted;
  }

  // Only convert Iterator to List if we have to (i.e. when "add" or "sort" is called),
  // otherwise keep as lazy iterator for as long as possible
  private List<KV<byte[], byte[]>> materializedItems() {
    if (items == null) {
      items = new ArrayList<>();
    } else if (MergeSortingIterable.class.isAssignableFrom(items.getClass())) {
      final List<KV<byte[], byte[]>> itemList = new ArrayList<>();
      items.iterator().forEachRemaining(itemList::add);
      items = itemList;
    }

    return (List<KV<byte[], byte[]>>) items;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public void add(KV<byte[], byte[]> item) {
    isEmpty = false;

    if (isSorted) {
      boolean isAdded = false;
      for (int i = 0; i < materializedItems().size(); i++) {
        if (comparator.compare(item, materializedItems().get(i)) <= 0) {
          materializedItems().add(i, item);
          isAdded = true;
          break;
        }
      }
      if (!isAdded) {
        materializedItems().add(item);
      }
    } else {
      materializedItems().add(item);
    }
  }

  public Iterable<KV<byte[], byte[]>> sorted() {
    if (!isSorted) {
      if (isEmpty) {
        items = new ArrayList<>();
      } else {
        materializedItems().sort(comparator);
      }
      isSorted = true;
    }
    return materializedItems();
  }

  static class SorterCoder extends AtomicCoder<SortingAccumulator> {
    private static final Coder<Iterable<KV<byte[], byte[]>>> ITERABLE_CODER =
        IterableCoder.of(KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()));
    private static final Coder<Boolean> BOOLEAN_CODER = BooleanCoder.of();
    private static final SorterCoder INSTANCE = new SorterCoder();

    static SorterCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(SortingAccumulator value, OutputStream outStream) throws IOException {
      BOOLEAN_CODER.encode(value.isEmpty, outStream);

      // Always encode the sorted list
      if (!value.isEmpty) {
        ITERABLE_CODER.encode(value.sorted(), outStream); // Encodes as a List
      }
    }

    @Override
    public SortingAccumulator decode(InputStream inStream) throws IOException {
      final boolean isEmpty = BOOLEAN_CODER.decode(inStream);

      if (!isEmpty) { // If items have already been added
        return new SortingAccumulator(false, ITERABLE_CODER.decode(inStream), true);
      } else {
        return new SortingAccumulator();
      }
    }
  }
}
