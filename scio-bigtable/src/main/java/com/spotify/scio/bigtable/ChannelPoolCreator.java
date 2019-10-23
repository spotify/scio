/*
 * Copyright 2016 Spotify AB.
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
import io.grpc.ClientInterceptor;

import java.io.IOException;

public class ChannelPoolCreator {
  public static final BigtableOptions options = new BigtableOptions.Builder().build();
  private static ClientInterceptor[] interceptors;

  static {
    try {
      final ClientInterceptor interceptor = CredentialInterceptorCache
        .getInstance()
        .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());

      // If credentials are unset (i.e. via local emulator), CredentialsInterceptor will return null
      if (interceptor == null) {
        interceptors = new ClientInterceptor[] {};
      } else {
        interceptors = new ClientInterceptor[] { interceptor };
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static ChannelPool createPool(final BigtableOptions options) throws IOException {
    return new ChannelPool(() ->
        BigtableSession.createNettyChannel(options.getAdminHost(), options, interceptors), 1);
  }
}
