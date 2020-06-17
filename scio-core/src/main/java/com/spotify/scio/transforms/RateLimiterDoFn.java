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

package com.spotify.scio.transforms;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.RateLimiter;

/**
 * DoFn which will rate limit the number of elements processed per second.
 *
 * <p>Used to rate limit throughput for a job writing to a database or making calls to external
 * services. The limit is applied per worker and should be used with a fixed/max num workers. Having
 * RateLimiterDoFn(1000) and 20 workers means your total rate will be 20000.
 */
public class RateLimiterDoFn<InputT> extends DoFnWithResource<InputT, InputT, RateLimiter> {

  private final double recordsPerSecond;

  public RateLimiterDoFn(final double recordsPerSecond) {
    this.recordsPerSecond = recordsPerSecond;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    getResource().acquire();
    c.output(c.element());
  }

  @Override
  public DoFnWithResource.ResourceType getResourceType() {
    return ResourceType.PER_CLASS;
  }

  @Override
  public RateLimiter createResource() {
    return RateLimiter.create(recordsPerSecond);
  }
}
