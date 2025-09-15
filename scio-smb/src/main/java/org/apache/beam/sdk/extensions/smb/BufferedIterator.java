/*
 * Copyright 2021 Spotify AB.
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

import java.util.Iterator;
import java.util.NoSuchElementException;

/** Buffers the underlying iterator in chunks to avoid IO thrashing. */
class BufferedIterator<T> implements Iterator<T> {
  private final Iterator<T> internal;
  private final int bufferSize;
  private final Object[] buffer;
  private int idx;
  private int size;

  public BufferedIterator(Iterator<T> internal, int bufferSize) {
    this.internal = internal;
    this.bufferSize = bufferSize;
    buffer = new Object[bufferSize];
    refill();
  }

  private void refill() {
    idx = 0;
    size = 0;
    while (size < bufferSize && internal.hasNext()) {
      buffer[size++] = internal.next();
    }
  }

  @Override
  public boolean hasNext() {
    return size > 0;
  }

  @Override
  public T next() {
    if (idx >= size) {
      throw new NoSuchElementException();
    }
    @SuppressWarnings("unchecked")
    T result = (T) buffer[idx++];
    if (idx == size) {
      refill();
    }
    return result;
  }
}
