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

import com.spotify.scio.testing._
import org.elasticsearch.action.index.IndexRequest

class ElasticsearchIOTest extends ScioIOSpec {
  "ElasticsearchIO" should "work with output" in {
    val xs = 1 to 100
    def opts(clusterName: String): ElasticsearchOptions = ElasticsearchOptions(clusterName, Nil)
    testJobTestOutput(xs)(c => ElasticsearchIO(opts(c))) { case (data, c) =>
      data.saveAsElasticsearch(opts(c))(x => Seq(new IndexRequest))
    }
  }
}
