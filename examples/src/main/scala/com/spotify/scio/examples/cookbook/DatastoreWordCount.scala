/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.api.services.datastore.DatastoreV1.{Query, Entity}
import com.google.api.services.datastore.client.DatastoreHelper
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.DatastoreWordCount
  --project=[PROJECT] --runner=BlockingDataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/datastore_wordcount
  --dataset=[PROJECT]
  --readOnly=false
*/

object DatastoreWordCount {
  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)

    // override runner to ensure sequential execution
    opts.setRunner(classOf[BlockingDataflowPipelineRunner])

    val kind = args.getOrElse("kind", "shakespeare-demo")
    val namespace = args.optional("namespace")
    val dataset = args("dataset")

    val ancestorKey = {
      val k = DatastoreHelper.makeKey(kind, "root")
      namespace.foreach(k.getPartitionIdBuilder.setNamespace)
      k.build()
    }

    def writeToDatastore(): Unit = {
      val sc = ScioContext(opts)
      sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
        .map { s =>
          val k = DatastoreHelper.makeKey(ancestorKey, kind, UUID.randomUUID().toString)
          namespace.foreach(k.getPartitionIdBuilder.setNamespace)
          Entity.newBuilder()
            .setKey(k.build())
            .addProperty(DatastoreHelper.makeProperty("content", DatastoreHelper.makeValue(s)))
            .build()
        }
        .saveAsDatastore(dataset)
      sc.close()
    }

    def readFromDatastore(): Unit = {
      val query = {
        val q = Query.newBuilder()
        q.addKindBuilder().setName(kind)
        q.build()
      }

      val sc = ScioContext(opts)
      sc.datastore(dataset, query)
        .flatMap(e => DatastoreHelper.getPropertyMap(e).asScala.get("content").map(_.getStringValue).toSeq)
        .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        .countByValue()
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
