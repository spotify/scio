package com.spotify.scio.bigtable;

import org.junit.Assert;
import org.junit.Test;

public class BigtableUtilTest {

  @Test
  public void simplifyTest() {
    Assert.assertEquals(
        BigtableUtil.simplify("/projects/scio-test/instances/test-instance/clusters/sample-cluster"),
        "sample-cluster"
    );

    Assert.assertEquals(
        BigtableUtil.simplify("simple-name-cluster"),
        "simple-name-cluster"
    );
  }
}
