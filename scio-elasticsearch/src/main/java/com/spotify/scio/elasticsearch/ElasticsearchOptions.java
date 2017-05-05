package com.spotify.scio.elasticsearch;

import java.net.InetSocketAddress;

public class ElasticsearchOptions {
  private final String clusterName;
  private final InetSocketAddress[] servers;
  public ElasticsearchOptions(String clusterName, InetSocketAddress[] servers) {
    this.clusterName = clusterName;
    this.servers = servers;
  }
  public String clusterName() {
    return clusterName;
  }
  public InetSocketAddress[] servers() {
    return servers;
  }
}