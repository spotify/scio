package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.api.services.datastore.DatastoreV1.Entity
import com.google.api.services.datastore.client.DatastoreHelper
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleOptions
import com.spotify.scio.values.SCollection
import org.joda.time.Duration

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.complete.AutoComplete
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --streaming=true
  --pubsubTopic=projects/[PROJECT]/topics/auto_complete
  --inputFile=gs://dataflow-samples/shakespeare/kinglear.txt
  --outputToBigqueryTable=true
  --outputBigqueryTable=[DATASET].auto_complete
  --outputToDatastore=false
*/

object AutoComplete {

  def computeTop(input: SCollection[(String, Long)], minPrefix: Int, maxPrefix: Int = Int.MaxValue)
  : SCollection[(String, Iterable[(String, Long)])] =
    input.flatMap { kv =>
      val (word, count) = kv
      (minPrefix to Math.min(word.length, maxPrefix)).map(i => (word.substring(0, i), (word, count)))
    }.topByKey(10)(Ordering.by(_._2))

  def computeTopRecursive(input: SCollection[(String, Long)], minPrefix: Int)
  : Seq[SCollection[(String, Iterable[(String, Long)])]] =
    if (minPrefix > 10) {
      computeTop(input, minPrefix).partition(2, t => if (t._1.length > minPrefix) 0 else 1)
    } else {
      val larger = computeTopRecursive(input, minPrefix + 1)
      val small = computeTop(larger(1).flatMap(_._2) ++ input.filter(_._1.length == minPrefix), minPrefix, minPrefix)
      Seq(larger.head ++ larger(1), small)
    }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)

    // set up example wiring
    val dataflowUtils = new DataflowExampleUtils(opts)

    val outputToDatastore = args.getOrElse("outputToDatastore", "false").toBoolean

    // initialize input
    val input = if (opts.isStreaming) {
      require(!outputToDatastore, "DatastoreIO is not supported in streaming.")
      dataflowUtils.setupPubsubTopic()
      sc.pubsubTopic(opts.getPubsubTopic).withSlidingWindows(Duration.standardMinutes(30))
    } else {
      sc.textFile(args("inputFile"))
    }

    // compute candidates
    val candidates = input.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty).map(_.toLowerCase)).countByValue()
    val tags = if (args.getOrElse("recursive", "true").toBoolean) {
      SCollection.unionAll(computeTopRecursive(candidates, 1))
    } else {
      computeTop(candidates, 1)
    }

    // write output to BigQuery
    if (args.getOrElse("outputToBigqueryTable", "true").toBoolean) {
      dataflowUtils.setupBigQueryTable()
      val tagFields = List(
        new TableFieldSchema().setName("count").setType("INTEGER"),
        new TableFieldSchema().setName("tag").setType("STRING"))
      val fields = List(
        new TableFieldSchema().setName("pre").setType("STRING"),
        new TableFieldSchema().setName("tags").setType("RECORD").setMode("REPEATED").setFields(tagFields.asJava))
      val schema = new TableSchema().setFields(fields.asJava)
      tags
        .map { kv =>
          val tags = kv._2.map(p => TableRow("tag" -> p._1, "count" -> p._2))
          TableRow("pre" -> kv._1, "tags" -> tags.toList.asJava)
        }
        .saveAsBigQuery(args("outputBigqueryTable"), schema)
    }

    // write output to Datastore
    if (outputToDatastore) {
      val kind = args.getOrElse("kind", "autocomplete-demo")
      tags
        .map { kv =>
          val key = DatastoreHelper.makeKey(kind, kv._1).build();
          val candidates = kv._2.map { p =>
            DatastoreHelper.makeValue(Entity.newBuilder()
              .addProperty(DatastoreHelper.makeProperty("tag", DatastoreHelper.makeValue(p._1)))
              .addProperty(DatastoreHelper.makeProperty("count", DatastoreHelper.makeValue(p._2)))
            ).setIndexed(false).build()
          }
          Entity.newBuilder()
            .setKey(key)
            .addProperty(DatastoreHelper.makeProperty("candidates", DatastoreHelper.makeValue(candidates.asJava)))
            .build()
        }
        .saveAsDatastore(opts.getProject)
    }

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    if (opts.isStreaming) {
      args.optional("inputFile").foreach { inputFile =>
        dataflowUtils.runInjectorPipeline(inputFile, opts.getPubsubTopic)
      }
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
}
