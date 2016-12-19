package com.spotify.scio.values

import com.spotify.scio.testing.ITPipelineSpec

class DistCacheTest extends ITPipelineSpec {

  "GCS DistCache" should "work" in {
    val distCacheUri = "gs://data-integration-test-us/scio/name-distcache"

    runWithDistCache(Seq("name1", "name2"), distCacheUri) { sc =>
      val names = sc.distCache(distCacheUri) { f =>
        scala.io.Source.fromFile(f).getLines().toList
      }

      val p = sc.parallelize(0 to 1)
        .map(i => (i, names()(i)))

      p should containInAnyOrder(Seq((0, "name1"), (1, "name2")))
    }
  }
}
