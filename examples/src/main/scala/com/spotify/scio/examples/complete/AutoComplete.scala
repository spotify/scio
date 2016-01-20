package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.api.services.datastore.DatastoreV1.Entity
import com.google.api.services.datastore.client.DatastoreHelper
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.{ExampleData, ExampleOptions}
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
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
  --outputToDatastore=false
*/

object AutoComplete {

  val bigQuerySchema: TableSchema = {
    val tagFields = List(
      new TableFieldSchema().setName("count").setType("INTEGER"),
      new TableFieldSchema().setName("tag").setType("STRING"))
    val fields = List(
      new TableFieldSchema().setName("pre").setType("STRING"),
      new TableFieldSchema().setName("tags").setType("RECORD").setMode("REPEATED").setFields(tagFields.asJava))
    new TableSchema().setFields(fields.asJava)
  }

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

  def makeEntity(kind: String, kv: (String, Iterable[(String, Long)])): Entity = {
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

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    if (opts.isStreaming) {
      opts.setRunner(classOf[DataflowPipelineRunner])
    }
    opts.setBigQuerySchema(bigQuerySchema)
    val dataflowUtils = new DataflowExampleUtils(opts)

    // arguments
    val inputFile = args.optional("inputFile")
    val outputToBigqueryTable = args.boolean("outputToBigqueryTable", true)
    val outputToDatastore = args.boolean("outputToDatastore", false)
    val kind = args.getOrElse("kind", "autocomplete-demo")

    val sc = ScioContext(opts)

    // initialize input
    val input = if (opts.isStreaming) {
      require(!outputToDatastore, "DatastoreIO is not supported in streaming.")
      dataflowUtils.setupPubsub()
      sc.pubsubTopic(opts.getPubsubTopic).withSlidingWindows(Duration.standardMinutes(30))
    } else {
      sc.textFile(inputFile.getOrElse(ExampleData.KING_LEAR))
    }

    // compute candidates
    val candidates = input.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty).map(_.toLowerCase)).countByValue()
    val tags = if (args.boolean("recursive", true)) {
      SCollection.unionAll(computeTopRecursive(candidates, 1))
    } else {
      computeTop(candidates, 1)
    }

    // outputs
    if (outputToBigqueryTable) {
      dataflowUtils.setupBigQueryTable()
      tags
        .map { kv =>
          val tags = kv._2.map(p => TableRow("tag" -> p._1, "count" -> p._2))
          TableRow("pre" -> kv._1, "tags" -> tags.toList.asJava)
        }
        .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), bigQuerySchema)
    }
    if (outputToDatastore) {
      tags
        .map(makeEntity(kind, _))
        .saveAsDatastore(opts.getProject)
    }

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    if (opts.isStreaming) {
      inputFile.foreach { f =>
        dataflowUtils.runInjectorPipeline(f, opts.getPubsubTopic)
      }
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

}
