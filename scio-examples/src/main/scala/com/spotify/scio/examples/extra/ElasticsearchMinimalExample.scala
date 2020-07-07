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

// Example: Elasticsearch minimal example

// Open a connection to an ES cluster and index
// the word count of a input document

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.ElasticsearchMinimalExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=[INPUT] --index=[INDEX] --esHost=[HOST] --esPort=[PORT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.elasticsearch._
import com.spotify.scio.examples.common.ExampleData

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

object ElasticsearchMinimalExample {
  def main(cliArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cliArgs)

    val host = args.getOrElse("esHost", "localhost")
    val port = args.getOrElse("esPort", "9200").toInt

    // Output es index to write into
    val index = args.getOrElse("index", "defaultindex")

    val primaryHost = new HttpHost(host, port)
    val servers: Seq[HttpHost] = Seq(primaryHost)

    val clusterOpts = ElasticsearchOptions(servers)

    // Provide an elasticsearch indexer to transform collections to indexable ES documents
    val indexRequestBuilder = indexer(index)

    // Open text file as `SCollection[String]`. The input can be either a single file or a
    // wildcard matching multiple files.
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .transform("counter") {
        // Split input lines, filter out empty tokens and expand into a collection of tokens
        _.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        // Count occurrences of each unique `String` to get `(String, Long)`
          .countByValue
      }
      // Save each collection as an ES document
      .saveAsElasticsearch(clusterOpts)(indexRequestBuilder)

    // Run pipeline
    sc.run().waitUntilFinish()
  }

  private val indexer = (index: String) =>
    (message: (String, Long)) => {
      val request = new IndexRequest(index)
        .id(message._1)
        .source(
          "user",
          "example",
          "postDate",
          new java.util.Date(),
          "word",
          message._1.toString,
          "count",
          message._2.toString
        )

      Iterable(request)
    }
}
