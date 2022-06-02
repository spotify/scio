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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelPoolCreator {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelPoolCreator.class);

  private static ClientInterceptor[] getClientInterceptors(final BigtableOptions options) {
    try {
      final ClientInterceptor interceptor =
          CredentialInterceptorCache.getInstance()
              .getCredentialsInterceptor(options.getCredentialOptions());
      // If credentials are unset (i.e. via local emulator), CredentialsInterceptor will return null
      if (interceptor == null) {
        return new ClientInterceptor[] {};
      } else {
        return new ClientInterceptor[] {interceptor};
      }
    } catch (Exception e) {
      LOG.error(
          "Failed to get credentials interceptor. No interceptor will be used for the channel.", e);
      return new ClientInterceptor[] {};
    }
  }

  public static ChannelPool createPool(final BigtableOptions options) throws IOException {
    final ClientInterceptor[] interceptors = getClientInterceptors(options);

    return new ChannelPool(
        () ->
            BigtableSession.createNettyChannel(
                options.getAdminHost(), options, false, interceptors),
        1);
  }
}
