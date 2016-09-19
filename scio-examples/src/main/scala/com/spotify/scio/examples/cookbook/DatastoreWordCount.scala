/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.examples.cookbook

import java.util.UUID

import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptions}
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.common.collect.ImmutableMap
import com.google.datastore.v1.{Entity, Query}
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

/*
sbt "scio-examples/runMain com.spotify.scio.examples.cookbook.DatastoreWordCount
  --project=[PROJECT] --runner=BlockingDataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/datastore_wordcount
  --dataset=[PROJECT]
  --readOnly=false"
*/

object DatastoreWordCount {
  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)

    // enforce sequential execution for this example, since we don't want
    // read pipeline to start before write pipeline has finished
    opts.setRunner(classOf[BlockingDataflowPipelineRunner])

    val kind = args.getOrElse("kind", "shakespeare-demo")
    val namespace = args.optional("namespace")
    val project = opts.as(classOf[DataflowPipelineOptions]).getProject

    val ancestorKey = {
      val key = makeKey(kind, "root")
      namespace.foreach(key.getPartitionIdBuilder.setNamespaceId)
      key.build()
    }

    // pipeline that writes to Datastore
    def writeToDatastore(): Unit = {
      val sc = ScioContext(opts)
      sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
        .map { s =>
          val key = makeKey(ancestorKey, kind, UUID.randomUUID().toString)
          namespace.foreach(key.getPartitionIdBuilder.setNamespaceId)
          Entity.newBuilder()
            .setKey(key.build())
            .putAllProperties(ImmutableMap.of("content", makeValue(s).build()))
            .build()
        }
        .saveAsDatastore(project)
      sc.close()
    }

    // pipeline that reads from Datastore
    def readFromDatastore(): Unit = {
      val query = {
        val q = Query.newBuilder()
        q.addKindBuilder().setName(kind)
        q.build()
      }

      val sc = ScioContext(opts)
      sc.datastore(project, query)
        .flatMap { e =>
          e.getProperties.asScala.get("content").map(_.getStringValue).toSeq
        }
        .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        .countByValue
        .map(t => t._1 + ": " + t._2)
        .saveAsTextFile(args("output"))
      sc.close()
    }

    if (!args.boolean("readOnly")) {
      writeToDatastore()
    }
    readFromDatastore()
  }
  // scalastyle:on method.length
}
