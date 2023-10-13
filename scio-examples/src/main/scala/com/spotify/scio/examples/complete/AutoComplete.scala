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
// --project=[PROJECT] --runner=DataflowPRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --outputToBigqueryTable=true --outputToDatastore=false --output=[DATASET].auto_complete"`
package com.spotify.scio.examples.complete

import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.datastore._
import com.spotify.scio.values.SCollection
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.{GlobalWindows, SlidingWindows}
import org.joda.time.Duration

import scala.jdk.CollectionConverters._

object AutoComplete {
  case class Tag(tag: String, count: Long)

  implicit val TagOrdering: Ordering[Tag] = Ordering.by(_.count)

  @BigQueryType.toTable
  case class Record(pre: String, tags: List[Tag])

  def computeTopCompletions(
    input: SCollection[String],
    candidatesPerPrefix: Int,
    recursive: Boolean
  ): SCollection[(String, Iterable[Tag])] = {
    val candidates = input.countByValue
      .map { case (word, count) => Tag(word, count) }
    if (recursive) {
      val (large, small) = computeTopRecursive(candidates, candidatesPerPrefix, 1)
      large ++ small
    } else {
      computeTopFlat(candidates, candidatesPerPrefix, 1)
    }
  }

  def computeTopFlat(
    input: SCollection[Tag],
    candidatesPerPrefix: Int,
    minPrefix: Int
  ): SCollection[(String, Iterable[Tag])] =
    input
      .flatMap(allPrefixes(minPrefix))
      .topByKey(candidatesPerPrefix)

  def computeTopRecursive(
    input: SCollection[Tag],
    candidatesPerPrefix: Int,
    minPrefix: Int
  ): (SCollection[(String, Iterable[Tag])], SCollection[(String, Iterable[Tag])]) =
    if (minPrefix > 10) {
      computeTopFlat(input, candidatesPerPrefix, minPrefix)
        .partition(t => t._1.length > minPrefix)
    } else {
      val (recLarge, recSmall) = computeTopRecursive(input, candidatesPerPrefix, minPrefix + 1)
      val large = recLarge ++ recSmall
      val small = (recSmall.flatMap(_._2) ++ input.filter(_.tag.length == minPrefix))
        .flatMap(allPrefixes(minPrefix, minPrefix))
        .topByKey(candidatesPerPrefix)
      (large, small)
    }

  def allPrefixes(
    minPrefix: Int,
    maxPrefix: Int = Int.MaxValue
  ): Tag => Iterable[(String, Tag)] = { tag =>
    val word = tag.tag
    val count = tag.count
    (minPrefix to Math.min(word.length, maxPrefix))
      .map(i => (word.substring(0, i), Tag(word, count)))
  }

  def makeEntity(kind: String, pre: String, tags: Iterable[Tag]): Entity = {
    val key = makeKey(kind, pre).build()
    val candidates = tags.map { p =>
      makeValue(
        Entity
          .newBuilder()
          .putAllProperties(
            Map("tag" -> makeValue(p.tag).build(), "count" -> makeValue(p.count).build()).asJava
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
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val exampleUtils = new ExampleUtils(sc.options)

    // arguments
    val input = args("input")
    val isRecursive = args.boolean("recursive", true)
    val outputToBigqueryTable = args.boolean("outputToBigqueryTable", true)
    val outputToDatastore = args.boolean("outputToDatastore", false)
    val isStreaming = sc.optionsAs[StreamingOptions].isStreaming

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
        .map { case (pre, tags) => Record(pre, tags.toList) }
        .saveAsTypedBigQueryTable(Table.Spec(args("output")))
    }
    if (outputToDatastore) {
      val kind = args.getOrElse("kind", "autocomplete-demo")
      tags
        .map { case (pre, tags) => makeEntity(kind, pre, tags) }
        .saveAsDatastore(sc.optionsAs[GcpOptions].getProject)
    }

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
