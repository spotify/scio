package com.spotify.scio.bigtable;

import org.junit.Assert;
import org.junit.Test;

public class BigtableUtilTest {

  @Test
  public void shorterNameTest() {
    Assert.assertEquals(
        BigtableUtil.shorterName(
            "/projects/scio-test/instances/test-instance/clusters/sample-cluster"),
        "sample-cluster");

    Assert.assertEquals(BigtableUtil.shorterName("simple-name-cluster"), "simple-name-cluster");
  }
}
