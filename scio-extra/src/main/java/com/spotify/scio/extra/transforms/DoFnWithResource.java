/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra.transforms;

import com.google.common.collect.Maps;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link DoFn} that manages an external resource.
 */
public abstract class DoFnWithResource<InputT, OutputT, ResourceT> extends DoFn<InputT, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(DoFnWithResource.class);
  private static final ConcurrentMap<String, Object> resources = Maps.newConcurrentMap();

  private final String instanceId;
  private String resourceId = null;

  /**
   * Resource type for sharing the resource among {@link DoFn} instances.
   *
   * {@link DoFn}s are defined and created locally, serialized, submitted to the runner and
   * de-serialized on remote workers.
   *
   * <pre><code>
   * // Define a sub-class
   * {@literal class MyDoFn extends DoFnWithResource<String, String, MyResource>} {
   *   ...
   * }
   *
   * // Create instances
   * MyDoFn f1 = new MyDoFn();
   * MyDoFn f2 = new MyDoFn();
   *
   * {@literal PCollection<String>} lines = ...;
   *
   * // Clone instances to remote workers
   * // On a 4-core Dataflow worker, both `f1` and `f2` are cloned 4 times, one for each CPU core.
   * lines.apply(ParDo.of(f1));
   * lines.apply(ParDo.of(f2));
   * </code></pre>
   */
  public enum ResourceType {
    /**
     * One instance of the resource per sub-class.
     *
     * All instances of the same class in a remote JVM share a single `ResourceT`, e.g. all clones
     * of `f1` and `f2`. This is useful for sharing resources within the same JVM, e.g. static
     * look up table, thread-safe asynchronous client.
     */
    PER_CLASS,

    /**
     * One instance of the resource per sub-class instance.
     *
     * Every instance of the same class in a remote JVM share a single `ResourceT`, e.g. all clones
     * of `f1` share one and all clones of `f2` share a different one. This is useful for sharing
     * resources within each {@link ParDo} transform, e.g. accumulation specific to current
     * transform logic.
     */
    PER_INSTANCE,

    /**
     * One instance of the resource per cloned instance.
     *
     * Each cloned instance in a remote JVM has its own copy of `ResourceT`, e.g. each clone of
     * `f1` and `f2` has its own copy. This is useful for thread local resources, e.g. resources
     * that are not thread-safe.
     */
    PER_CLONE
  }

  /**
   * Get resource type.
   */
  public abstract ResourceType getResourceType();

  /**
   * Create resource.
   *
   * {@link DoFnWithResource#getResourceType()} determines how many times this is called.
   */
  public abstract ResourceT createResource();

  protected DoFnWithResource() {
    this.instanceId = this.getClass().getName() + "-" + UUID.randomUUID().toString();
  }

  @Setup
  public void setup() {
    switch (getResourceType()) {
      case PER_CLASS:
        resourceId = this.getClass().getName();
        break;
      case PER_INSTANCE:
        resourceId = instanceId;
        break;
      case PER_CLONE:
        resourceId = instanceId + "-" + this.toString();
        break;
    }
    resources.computeIfAbsent(resourceId, key -> {
      LOG.info("Creating resource {}", resourceId);
      return createResource();
    });
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("Resource Type", getResourceType().toString()));
  }

  /**
   * Get managed resource.
   */
  @SuppressWarnings("unchecked")
  public ResourceT getResource() {
    return (ResourceT) resources.get(resourceId);
  }

}
