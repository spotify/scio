package com.spotify.scio.extra.sparkey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * MockCache is a global singleton Cache object that can be "serialized" for use in tests.
 * This allows us to verify the Cache is being used correctly when used with
 * CachedSparkeySideInput, as long as the test runner and the code under test are using the
 * same JVM. (The serialization of this Cache has been disabled, as all instances share the same
 * underlying Caffeine cache.)
 */
public class MockCache implements Cache<String, Object>, Externalizable {
  private static Cache<String, Object> cache = null;

  static {
    reset();
  }

  public static void reset() { cache = Caffeine.newBuilder().recordStats().build(); }

  public static MockCache getInstance() { return new MockCache(); }

  public static CacheStats getStats() { return cache.stats(); }

  @Nullable
  @Override
  public Object getIfPresent(final Object key) {
    return cache.getIfPresent(key);
  }

  @Nullable
  @Override
  public Object get(@NonNull final String key,
                       @NonNull final Function<? super String, ? extends Object> mappingFunction) {
    return cache.get(key, mappingFunction);
  }

  @Override
  public Map<String, Object> getAllPresent(final Iterable<?> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(final String key, final Object value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends String, ? extends Object> m) {
    cache.putAll(m);
  }

  @Override
  public void invalidate(final Object key) {
    cache.invalidate(key);
  }

  @Override
  public void invalidateAll(final Iterable<?> keys) {
    cache.invalidateAll(keys);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public @NonNegative long estimatedSize() {
    return 0;
  }

  @Override
  public CacheStats stats() {
    return cache.stats();
  }

  @Override
  public ConcurrentMap<String, Object> asMap() {
    return cache.asMap();
  }

  @Override
  public void cleanUp() {
    cache.cleanUp();
  }

  @Override
  public @NonNull Policy<String, Object> policy() {
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    // Not implemented on purpose, as all this class does is access the static cache on this class.
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    // Not implemented on purpose, as all this class does is access the static cache on this class.
  }
}
