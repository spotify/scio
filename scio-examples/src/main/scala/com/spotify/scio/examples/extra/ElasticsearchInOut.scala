/*
 * Copyright 2019 Spotify AB.
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

// Example: Elasticsearch Input and Output
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.ElasticsearchInOut
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=[INPUT] --index=[INDEX] --esHost=[HOST] --esPort=[PORT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.elasticsearch._

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

object ElasticsearchInOut {
  def main(cliArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cliArgs)

    val input = args.required("input")

    val host = args.getOrElse("esHost", "localhost")
    val port = args.getOrElse("esPort", "9200").toInt

    // Output es index to write into
    val index = args.getOrElse("index", "defaultIndex")

    val primaryHost = new HttpHost(host, port)
    val servers: Seq[HttpHost] = Seq(primaryHost)

    val clusterOpts = ElasticsearchOptions(servers)

    // Provide an elasticsearch writer to transform collections to es documents
    val indexWriter = indexer(index)

    // Read a simple text file and write the first 10 characters to an elasticsearch document
    ElasticsearchSCollection(sc.textFile(input))
      .saveAsElasticsearch(clusterOpts)(indexWriter)

    // Run pipeline
    val result = sc.run().waitUntilFinish()
  }

  private val indexer = (index: String) =>
    (message: String) => {
      val request = new IndexRequest(index)
        .id("1")
        .source(
          "user",
          "example",
          "postDate",
          new java.util.Date(),
          "message",
          message.substring(0, 10)
        )

      Iterable(request)
    }
}
