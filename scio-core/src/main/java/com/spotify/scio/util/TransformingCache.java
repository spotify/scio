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

package com.spotify.scio.util;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Cache;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableMap;

import javax.annotation.CheckForNull;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Similar to {@link com.google.common.cache.ForwardingCache} but with key transformation using the
 * injective function {@link #transformKey(K)} from {@link K} to {@link U}. <br>
 * This cache won't be able to return original keys, hence {@link #getAllPresent(Iterable)} and
 * {@link #asMap()} both throw an {@link UnsupportedOperationException}.
 *
 * @param <K> the type of the cache's keys, which are not permitted to be null
 * @param <U> the type of the underlying cache's keys, which are not permitted to be null
 * @param <V> the type of the cache's values, which are not permitted to be null
 */
public abstract class TransformingCache<K, U, V> extends ForwardingObject implements Cache<K, V> {

  protected abstract Cache<U, V> delegate();

  protected abstract U transformKey(K key);

  private Iterable<U> transformKeys(Iterable<?> keys) {
    return StreamSupport.stream(keys.spliterator(), false)
        .map(k -> (K) k)
        .map(this::transformKey)
        .collect(Collectors.toSet());
  }

  @CheckForNull
  @Override
  public V getIfPresent(Object key) {
    return delegate().getIfPresent(transformKey((K) key));
  }

  @Override
  public V get(K key, Callable<? extends V> loader) throws ExecutionException {
    return delegate().get(transformKey(key), loader);
  }

  @Override
  public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(K key, V value) {
    delegate().put(transformKey(key), value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    final Map<U, V> t =
        m.entrySet().stream()
            .collect(Collectors.toMap(e -> transformKey(e.getKey()), Map.Entry::getValue));
    delegate().putAll(t);
  }

  @Override
  public void invalidate(Object key) {
    delegate().invalidate(transformKey((K) key));
  }

  @Override
  public void invalidateAll(Iterable<?> keys) {
    delegate().invalidateAll(transformKeys(keys));
  }

  @Override
  public void invalidateAll() {
    delegate().invalidateAll();
  }

  @Override
  public long size() {
    return delegate().size();
  }

  @Override
  public CacheStats stats() {
    return delegate().stats();
  }

  @Override
  public ConcurrentMap<K, V> asMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanUp() {
    delegate().cleanUp();
  }

  public abstract static class SimpleTransformingCache<K, U, V> extends TransformingCache<K, U, V> {
    private final Cache<U, V> delegate;

    protected SimpleTransformingCache(Cache<U, V> delegate) {
      this.delegate = Preconditions.checkNotNull(delegate);
    }

    @Override
    protected final Cache<U, V> delegate() {
      return delegate;
    }
  }
}
