package com.spotify.scio.examples.cookbook

import java.util.UUID

import com.google.api.services.datastore.DatastoreV1.{Query, Entity}
import com.google.api.services.datastore.client.DatastoreHelper
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.spotify.scio._

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.DatastoreWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/dataflow/datastore_wordcount
  --dataset=[PROJECT]
  --readOnly=false
*/

object DatastoreWordCount {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
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
      sc.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
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

}
