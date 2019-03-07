package com.spotify.scio.jmh.j;

final public class UserId {

  private Byte[] bytes;

  public UserId() {}

  public UserId(Byte[] bytes) {
    this.bytes = bytes;
  }

  public Byte[] getBytes() {
    return this.bytes;
  }

  public void setBytes(Byte[] bytes) {
    this.bytes = bytes;
  }
}