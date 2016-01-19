package com.spotify.scio.examples

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo, View}
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.scio._
import com.spotify.scio.bigquery._

// scalastyle:off
object SideInputTest {
  def main(cmdlineArgs: Array[String]): Unit = {
    sys.props(BigQueryClient.STAGING_DATASET_LOCATION_KEY) = "EU"
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val tm = sc.bigQuerySelect(
      """
        |SELECT track_gid AS t, master_metadata.albumartist.gid AS a
        |FROM [di-bigquery-data-commons:track_entity.track_entity_20160101]
        |WHERE global_popularity.percentile >= 99
        |LIMIT 10000
      """.stripMargin)
        .map(r => (r.get("t").toString, r.get("a").toString))

    val esc = sc.bigQuerySelect(
      """
        |SELECT play_track AS t
        |FROM [di-bigquery-data-commons:end_song_cleaned_decrypted.end_song_cleaned_decrypted_20160101]
        |WHERE play_track IS NOT NULL AND ms_played >= 300000
        |LIMIT 1000000
      """.stripMargin)
        .map(_.get("t").toString)

    val s1 = tm.asMapSideInput
    esc.withSideInputs(s1)
      .flatMap { case (t, s) =>
        s(s1).get(t).map(a => (t, a))
      }
      .toSCollection
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(args("output") + "/s1", numShards = 1)

    /*
    val s2 = tm.asIterableSideInput
    esc.withSideInputs(s2)
      .flatMap { case (t, s) =>
        s(s2).map(kv => (t, kv._1, kv._2))
      }
      .toSCollection
      .filter(t => t._1 == t._2)
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(args("output") + "/s2", numShards = 1)
      */

    val s3 = tm.asListSideInput
    esc.withSideInputs(s3)
      .flatMap { case (t, s) =>
        s(s3).map(kv => (t, kv._1, kv._2))
      }
      .toSCollection
      .filter(t => t._1 == t._2)
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(args("output") + "/s3", numShards = 1)

    val s4 = tm.groupBy(_ => 0).values.map(_.toMap).asSingletonSideInput
    esc.withSideInputs(s4)
      .flatMap { case (t, s) =>
        s(s4).get(t).map(a => (t, a))
      }
      .toSCollection
      .map(_.productIterator.mkString("\t"))
      .saveAsTextFile(args("output") + "/s4", numShards = 1)

    sc.close()

    /*
    val pipeline = sc.pipeline
    val s = tm.map(kv => KV.of(kv._1, kv._2)).internal.apply(View.asMap())

    esc.internal
      .apply(ParDo.withSideInputs(s).of(new DoFn[String, String]() {
        override def processElement(c: DoFn[String, String]#ProcessContext): Unit = {
          val m = c.sideInput(s)
          val x = c.element()
          val a = m.get(x)
          if (a != null) {
            c.output(x + "\t" + a)
          }
        }
      }))
      .apply(TextIO.Write.withNumShards(1).to(args("output")))
    pipeline.run()*/
  }
}
// scalastyle:on