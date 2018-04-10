/*
 * Copyright 2018 Spotify AB.
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

// Example: Compute TF-IDF from a Text Corpus
// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.TfIdf
// --project=[PROJECT] --runner=DataflowPRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/?*.txt
// --output=gs://[BUCKET]/[PATH]/tf_idf"`
package com.spotify.scio.examples.complete

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileSystems

import scala.collection.JavaConverters._
import scala.io.Source

object TfIdf {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Initialize `FileSystems`
    FileSystems.setDefaultPipelineOptions(sc.options)

    // List files from the input path
    val uris = FileSystems
      .`match`(args.getOrElse("input", ExampleData.SHAKESPEARE_ALL))
      .metadata().asScala.map(_.resourceId().toString)

    // Read files as a collection of `(doc, line)` where doc is the URI of the file
    val uriToContent = args.getOrElse("mode", "union") match {
      // Read each file separately with `sc.textFile`, key by URI, and then merge with
      // `SCollection.unionAll`. This works well for a small list of files. It can also handle
      // large individual files as each `sc.textFile` can be dynamically split by the runner.
      case "union" =>
        SCollection.unionAll(uris.map(uri => sc.textFile(uri).keyBy(_ => uri)))
      // Create a single `SCollection[String]` of URIs, and read files on workers in `.map` and
      // then key by URI. This wo rks well for a large list of files. However each file is read by
      // one worker thread and cannot be further split. Skew in file sizes may lead to unbalanced
      // work load among workers.
      case "fs" =>
        sc.parallelize(uris)
          .flatMap { uri =>
            // Read file with the `FileSystems` API inside a worker
            val rsrc = FileSystems.matchSingleFileSpec(uri).resourceId()
            val in = Channels.newInputStream(FileSystems.open(rsrc))
            Source.fromInputStream(in)
              .getLines()
              .map((uri, _))
          }
      // TODO: add splittable DoFn approach once available
      case m => throw new IllegalArgumentException(s"Unkonwn mode: $m")
    }

    computeTfIdf(uriToContent)
      .map { case (t, (d, tfIdf)) =>
        s"$t\t$d\t$tfIdf"
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }

  // Compute TF-IDF from an input collection of `(doc, line)`
  def computeTfIdf(uriToContent: SCollection[(String, String)])
  : SCollection[(String, (String, Double))] = {
    // Split lines into terms as (doc, term)
    val uriToWords = uriToContent.flatMap { case (uri, line) =>
      line.split("\\W+").filter(_.nonEmpty).map(w => (uri, w.toLowerCase))
    }

    val uriToWordAndCount = uriToWords
      // Count `(doc, terms)` occurrences to get `((doc, term), term-freq)`
      .countByValue
      // Remap tuple to key on doc, i.e. `(doc, (term, term-freq))`
      .map(t => (t._1._1, (t._1._2, t._2)))

    val wordToDf = uriToWords
      // Compute unique `(doc, term)` pairs
      .distinct
      // Drop keys (`doc`) and keep values (`term`)
      .values
      // Count `term` occurrences to get `(term, doc-freq)`
      .countByValue
      // Cross product with unique number of `doc`s, or `N`
      .cross(uriToContent.keys.distinct.count)
      // Compute `(term, DF)`
      .map { case ((t, df), numDocs) => (t, df.toDouble / numDocs) }

    uriToWords
      // Drop values (`term`) and keep keys (`doc`)
      .keys
      // Count `doc` occurrences to get `(doc, doc-length)`
      .countByValue
      // Join with `(doc, (term, term-freq))` to get `(doc, (doc-length, (term, term-freq)))`
      .join(uriToWordAndCount)
      // Compute `(term, (doc, TF))`
      .map { case (d, (dl, (t, tf))) => (t, (d, tf.toDouble / dl)) }
      // Join with `(term, DF)` to get `(term, ((doc, TF), DF))`
      .join(wordToDf)
      // Compute `(term, (doc, TF-IDF))`
      .map { case (t, ((d, tf), df)) => (t, (d, tf * math.log(1 / df))) }
  }

}
