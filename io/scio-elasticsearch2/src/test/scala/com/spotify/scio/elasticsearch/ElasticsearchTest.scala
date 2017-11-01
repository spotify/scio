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

package com.spotify.scio.elasticsearch

import java.net.InetSocketAddress

import com.spotify.scio._
import com.spotify.scio.testing._
import org.elasticsearch.action.index.IndexRequest
import org.joda.time.Duration

object ElasticsearchJobSpec {
  val options = ElasticsearchOptions("clusterName", Seq(new InetSocketAddress(8080)))
  val data = Seq(1, 2, 3)
  val shard = 2
  val maxBulkRequestSize = 1000
  val flushInterval = Duration.standardSeconds(1)
}

object ElasticsearchJob {
  import ElasticsearchJobSpec._
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(data)
      .saveAsElasticsearch(options, flushInterval, shard, maxBulkRequestSize, _ => ()) { _ =>
        new IndexRequest :: Nil
      }
    sc.close()
  }
}

class ElasticsearchTest extends PipelineSpec {
  import ElasticsearchJobSpec._
  "ElasticsearchIO" should "work" in {
    JobTest[ElasticsearchJob.type]
      .output(ElasticsearchIO[Int](options))(_ should containInAnyOrder (data))
      .run()
  }
}
