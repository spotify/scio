// INCOMPLETE
package com.spotify.scio.examples

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.api.services.datastore.DatastoreV1.Entity
import com.google.api.services.datastore.client.DatastoreHelper
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.joda.time.Duration

import scala.collection.JavaConverters._

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
      Seq(larger(0) ++ larger(1), small)
    }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val input = if (args.optional("inputFile").isDefined) {
      context.textFile(args("inputFile"))
    } else {
      context.options.setStreaming(true)
      context.pubsubTopic(args("inputTopic")).withSlidingWindows(Duration.standardMinutes(30))
    }

//    val candidates = input.flatMap(l => "#\\S+".r.findAllIn(l).map(_.substring(1))).countByValue()
    val candidates = input.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty).map(_.toLowerCase)).countByValue()

    val tags = if (args("recursive").toBoolean) {
      SCollection.unionAll(computeTopRecursive(candidates, 1))
    } else {
      computeTop(candidates, 1)
    }

    if (args.optional("outputBigqueryTable").isDefined) {
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

    if (args.optional("outputDataset").isDefined) {
      val kind = args("kind")
      tags
        .map { kv =>
          val ancestorKey = DatastoreHelper.makeKey(kind, "root").build()
          val key = DatastoreHelper.makeKey(ancestorKey, kv._1).build()

          val candidates = kv._2.map { p =>
            DatastoreHelper.makeValue(Entity.newBuilder()
              .addProperty(DatastoreHelper.makeProperty("tag", DatastoreHelper.makeValue(p._1)))
              .addProperty(DatastoreHelper.makeProperty("count", DatastoreHelper.makeValue(p._2)))
            ).build()
          }

          Entity.newBuilder()
            .setKey(key)
            .addProperty(DatastoreHelper.makeProperty("candidates", DatastoreHelper.makeValue(candidates.asJava)))
            .build()
        }
        .saveAsDatastore(args("outputDataset"))
    }

    context.close()
  }
}
