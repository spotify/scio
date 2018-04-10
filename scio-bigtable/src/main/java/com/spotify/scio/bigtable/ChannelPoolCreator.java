/*
 * Copyright 2018 Spotify AB.
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


package com.spotify.scio.bigtable;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.common.collect.ImmutableList;
import io.grpc.ClientInterceptor;

import java.io.IOException;
import java.util.List;

public class ChannelPoolCreator {
  public static final BigtableOptions options = new BigtableOptions.Builder().build();
  public static ClientInterceptor interceptor;

  static {
    try {
      interceptor = CredentialInterceptorCache.getInstance()
              .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static ChannelPool createPool(final String host) throws IOException {
    return new ChannelPool(() -> BigtableSession.createNettyChannel(host, options, interceptor),
        1);
  }
}