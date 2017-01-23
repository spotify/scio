/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.values

import java.nio.ByteBuffer

import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.MimeTypes
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.testing.PipelineSpec

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class DistCacheIT extends PipelineSpec {

  val distCacheUri = "gs://data-integration-test-us/scio/name-distcache"
  val cacheData = Seq("name1", "name2")

  "GCS DistCache" should "work" in {
    runWithDistCache(cacheData, distCacheUri) { (sc, names) =>
      val p = sc.parallelize(0 to 1)
        .map(i => (i, names()(i)))

      p should containInAnyOrder(Seq((0, "name1"), (1, "name2")))
    }
  }

  def runWithDistCache[T: ClassTag](data: Iterable[String], path: String)
                                   (fn: (ScioContext, DistCache[List[String]]) => T)
  : ScioResult = {
    val sc = ScioContext()
    val cache = sc.distCache(distCacheUri) { f =>
      scala.io.Source.fromFile(f).getLines().toList
    }
    val gcsUtil = new GcsUtilFactory().create(sc.options)
    try {
      val channel = gcsUtil.create(GcsPath.fromUri(path), MimeTypes.TEXT)
      channel.write(ByteBuffer.wrap(data.mkString("\n").getBytes))
      channel.close()
      fn(sc, cache)
      sc.close()
    }
    finally {
      val gcsPath = GcsPath.fromUri(path)
      if (gcsUtil.bucketExists(gcsPath)) {
        gcsUtil.remove(Seq(path).asJava)
      }
    }
  }
}
