package com.spotify.scio.elasticsearch;

import java.io.Serializable;

public class IndexRequestWrapper implements Serializable {

  private final String index;
  private final String type;
  private final byte[] source;
  private String id;
  private String timestamp;
  private Long ttl;
  private long version;
  private boolean refresh;

  private IndexRequestWrapper(IndexRequestWrapperBuilder builder) {
    this.index = builder.index;
    this.type = builder.type;
    this.source = builder.source;
    this.id = builder.id;
    this.timestamp = builder.timestamp;
    this.ttl = builder.ttl;
    this.version = builder.version;
    this.refresh = builder.refresh;
  }
  public String index() {
    return id;
  }
  public String type() {
    return type;
  }
  public byte[] source() {
    return source;
  }
  public String id() {
    return id;
  }
  public String timestamp() {
    return timestamp;
  }
  public Long ttl() {
    return ttl;
  }
  public long version() {
    return version;
  }
  public boolean refresh() {
    return refresh;
  }

  public static class IndexRequestWrapperBuilder {
    private final String index;
    private final String type;
    private final byte[] source;
    private String id;
    private String timestamp;
    private Long ttl;
    private long version;
    private boolean refresh;

    public IndexRequestWrapperBuilder(String index, String type, byte[] source) {
      this.index = index;
      this.type = type;
      this.source = source;
    }
    public IndexRequestWrapperBuilder id(String id) {
      this.id = id;
      return this;
    }
    public IndexRequestWrapperBuilder timestamp(String timestamp) {
      this.timestamp = timestamp;
      return this;
    }
    public IndexRequestWrapperBuilder ttl(Long ttl) {
      this.ttl = ttl;
      return this;
    }
    public IndexRequestWrapperBuilder version(long version) {
      this.version = version;
      return this;
    }
    public IndexRequestWrapperBuilder refresh(boolean refresh) {
      this.refresh = refresh;
      return this;
    }
    public IndexRequestWrapper build() {
      return new IndexRequestWrapper(this);
    }
  }
}
