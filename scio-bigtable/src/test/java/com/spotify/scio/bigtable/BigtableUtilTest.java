package com.spotify.scio.bigtable;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.cloud.bigtable.grpc.BigtableClusterUtilities;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class BigtableUtilTest {

  @Test
  @PrepareForTest({BigtableClusterUtilities.class, Cluster.class, ListClustersResponse.class})
  public void testGetClusterSizes() throws IOException, GeneralSecurityException {
    Cluster c1 = PowerMockito.mock(Cluster.class);
    when(c1.getName()).thenReturn("projects/p1/instances/i1/clusters/c1");
    when(c1.getServeNodes()).thenReturn(0);

    Cluster c2 = PowerMockito.mock(Cluster.class);
    when(c2.getName()).thenReturn("projects/p1/instances/i1/clusters/c2");
    when(c2.getServeNodes()).thenReturn(1);

    ListClustersResponse listClustersResponse = mock(ListClustersResponse.class);
    when(listClustersResponse.getClustersList()).thenReturn(Arrays.asList(c1, c2));

    BigtableClusterUtilities btUtil = mock(BigtableClusterUtilities.class);
    when(btUtil.getClusters()).thenReturn(listClustersResponse);

    PowerMockito.mockStatic(BigtableClusterUtilities.class);
    when(BigtableClusterUtilities.forInstance("p1", "i1")).thenReturn(btUtil);

    Map<String, Integer> sizes = BigtableUtil.getClusterSizes("p1", "i1");
    assertEquals(2, sizes.size());
    assertEquals(0, sizes.get("c1").intValue());
    assertEquals(1, sizes.get("c2").intValue());
  }

}
