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

import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio._
import com.spotify.scio.elasticsearch._
import com.spotify.scio.examples.common.ExampleData
import org.apache.http.HttpHost

import java.time.LocalDate

object ElasticsearchMinimalExample {

  final case class Document(
    user: String,
    postDate: LocalDate,
    word: String,
    count: Long
  )

  def main(cliArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cliArgs)

    val host = args.getOrElse("esHost", "localhost")
    val port = args.int("esPort", 9200)

    // Output es index to write into
    val index = args.getOrElse("index", "defaultindex")

    val primaryHost = new HttpHost(host, port)
    val nodes = Seq(primaryHost)

    val clusterOpts = ElasticsearchOptions(
      nodes = nodes,
      mapperFactory = () => {
        val mapper = new JacksonJsonpMapper() // Use jackson for user json serialization
        mapper.objectMapper().registerModule(DefaultScalaModule) // Add scala support
        mapper.objectMapper().registerModule(new JavaTimeModule()) // Add java.time support
        mapper
      }
    )

    // Provide an elasticsearch indexer to transform collections to indexable ES documents
    val indexRequestBuilder = indexer(index)

    sc
      // Open text file as `SCollection[String]`. The input can be either a single file or a
      // wildcard matching multiple files.
      .textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      // Split input lines and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+"))
      // Filter out empty tokens
      .filter(_.nonEmpty)
      // Count occurrences of each unique `String` to get `(String, Long)`
      .countByValue
      // Save each collection as an ES document
      .saveAsElasticsearch(clusterOpts)(indexRequestBuilder)

    // Run pipeline
    sc.run().waitUntilFinish()
  }

  private def indexer(index: String): ((String, Long)) => Iterable[BulkOperation] = {
    case (word, count) =>
      val document = Document(
        user = "example",
        postDate = LocalDate.now(),
        word = word,
        count = count
      )
      val op = IndexOperation.of[Document](_.index(index).document(document))
      Some(BulkOperation.of(_.index(op)))
  }
}
