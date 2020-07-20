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

// Example: AutoComplete lines of text
// Usage:

// `sbt "runMain com.spotify.scio.examples.complete.AutoComplete
// --project=[PROJECT] --runner=DataflowPRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --outputToBigqueryTable=true --outputToDatastore=false --output=[DATASET].auto_complete"`
package com.spotify.scio.examples.complete

import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.{GlobalWindows, SlidingWindows}
import org.joda.time.Duration

import scala.jdk.CollectionConverters._

object AutoComplete {
  case class Tag(tag: String, count: Long)

  @BigQueryType.toTable
  case class Record(pre: String, tags: List[Tag])

  def computeTopCompletions(
    input: SCollection[String],
    candidatesPerPrefix: Int,
    recursive: Boolean
  ): SCollection[(String, Iterable[(String, Long)])] = {
    val candidates = input.countByValue
    if (recursive) {
      SCollection.unionAll(computeTopRecursive(candidates, candidatesPerPrefix, 1))
    } else {
      computeTopFlat(candidates, candidatesPerPrefix, 1)
    }
  }

  def computeTopFlat(
    input: SCollection[(String, Long)],
    candidatesPerPrefix: Int,
    minPrefix: Int
  ): SCollection[(String, Iterable[(String, Long)])] =
    input
      .flatMap(allPrefixes(minPrefix))
      .topByKey(candidatesPerPrefix)(Ordering.by(_._2))

  def computeTopRecursive(
    input: SCollection[(String, Long)],
    candidatesPerPrefix: Int,
    minPrefix: Int
  ): Seq[SCollection[(String, Iterable[(String, Long)])]] =
    if (minPrefix > 10) {
      computeTopFlat(input, candidatesPerPrefix, minPrefix)
        .partition(2, t => if (t._1.length > minPrefix) 0 else 1)
    } else {
      val larger =
        computeTopRecursive(input, candidatesPerPrefix, minPrefix + 1)
      val small =
        (larger(1).flatMap(_._2) ++ input.filter(_._1.length == minPrefix))
          .flatMap(allPrefixes(minPrefix, minPrefix))
          .topByKey(candidatesPerPrefix)(Ordering.by(_._2))
      Seq(larger.head ++ larger(1), small)
    }

  def allPrefixes(
    minPrefix: Int,
    maxPrefix: Int = Int.MaxValue
  ): ((String, Long)) => Iterable[(String, (String, Long))] = {
    case (word, count) =>
      (minPrefix to Math.min(word.length, maxPrefix))
        .map(i => (word.substring(0, i), (word, count)))
  }

  def makeEntity(kind: String, kv: (String, Iterable[(String, Long)])): Entity = {
    val key = makeKey(kind, kv._1).build()
    val candidates = kv._2.map { p =>
      makeValue(
        Entity
          .newBuilder()
          .putAllProperties(
            Map("tag" -> makeValue(p._1).build(), "count" -> makeValue(p._2).build()).asJava
          )
      ).build()
    }
    Entity
      .newBuilder()
      .setKey(key)
      .putAllProperties(Map("candidates" -> makeValue(candidates.asJava).build()).asJava)
      .build()
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val exampleUtils = new ExampleUtils(opts)

    // arguments
    val input = args("input")
    val isRecursive = args.boolean("recursive", true)
    val outputToBigqueryTable = args.boolean("outputToBigqueryTable", true)
    val outputToDatastore = args.boolean("outputToDatastore", false)
    val isStreaming = opts.as(classOf[StreamingOptions]).isStreaming

    val sc = ScioContext(opts)

    // initialize input
    val windowFn = if (isStreaming) {
      require(!outputToDatastore, "DatastoreIO is not supported in streaming.")
      SlidingWindows
        .of(Duration.standardMinutes(30))
        .every(Duration.standardSeconds(5))
    } else {
      new GlobalWindows
    }

    val lines = sc
      .textFile(input)
      .flatMap("#\\S+".r.findAllMatchIn(_).map(_.matched))
      .withWindowFn(windowFn)

    val tags = computeTopCompletions(lines, 10, isRecursive)

    // outputs
    if (outputToBigqueryTable) {
      tags
        .map(kv => Record(kv._1, kv._2.map(p => Tag(p._1, p._2)).toList))
        .saveAsTypedBigQueryTable(Table.Spec(args("output")))
    }
    if (outputToDatastore) {
      val kind = args.getOrElse("kind", "autocomplete-demo")
      tags
        .map(makeEntity(kind, _))
        .saveAsDatastore(opts.as(classOf[GcpOptions]).getProject)
    }

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
