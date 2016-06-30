package com.spotify.scio.bigtable;

import com.google.bigtable.repackaged.com.google.cloud.config.BigtableOptions;
import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.cloud.grpc.io.ChannelPool;
import com.google.bigtable.repackaged.com.google.cloud.grpc.io.CredentialInterceptorCache;
import com.google.bigtable.repackaged.com.google.cloud.grpc.io.HeaderInterceptor;
import com.google.bigtable.repackaged.io.grpc.ManagedChannel;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

public class ChannelPoolCreator {
  public static final BigtableOptions options = new BigtableOptions.Builder().build();
  public static List<HeaderInterceptor> interceptors;

  static {
    try {
      interceptors = ImmutableList.of(CredentialInterceptorCache.getInstance()
              .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static ChannelPool createPool(final String host) throws IOException {
    return new ChannelPool(interceptors, new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return BigtableSession.createNettyChannel(host, options);
      }
    });
  }
}