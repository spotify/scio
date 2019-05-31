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

package com.spotify.scio.transforms;

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public abstract class AsyncDoFn<A, B, C, D, FutureT> extends DoFn<A, KV<A, D>> {
  // DoFn is deserialized once per CPU core. We assign a unique UUID to each DoFn instance upon
  // creation, so that all cloned instances share the same ID. This ensures all cores share the
  // same Client and Cache.
  protected static final ConcurrentMap<UUID, Object> client = Maps.newConcurrentMap();
  protected static final ConcurrentMap<UUID, Cache> cache = Maps.newConcurrentMap();
  protected final UUID instanceId;
  protected final AsyncLookupDoFn.CacheSupplier<A, B, ?> cacheSupplier;
  protected final Semaphore semaphore;
  protected long requestCount;
  protected long resultCount;

  public AsyncDoFn(final int maxPendingRequests,
                   final AsyncLookupDoFn.CacheSupplier<A, B, ?> cacheSupplier) {
    this.instanceId = UUID.randomUUID();
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  public void setup() {
    client.computeIfAbsent(instanceId, instanceId -> newClient());
    cache.computeIfAbsent(instanceId, instanceId -> cacheSupplier.createCache());
  }

  public void startBundle() {
    requestCount = 0;
    resultCount = 0;
  }

  /**
   * Creates client.
   */
  protected abstract C newClient();

  /**
   * Perform asynchronous lookup.
   */
  public abstract FutureT asyncLookup(C client, A input);
}
