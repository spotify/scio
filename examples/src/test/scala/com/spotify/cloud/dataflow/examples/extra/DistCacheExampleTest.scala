package com.spotify.cloud.dataflow.examples.extra

import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow.testing._
import org.joda.time.format.DateTimeFormat

class DistCacheExampleTest extends JobSpec {

  val fmt = DateTimeFormat.forPattern("yyyyMMdd")
  def d2t(date: String): Long = fmt.parseDateTime(date).getMillis / 1000

  val in = Seq(
    TableRow("timestamp" -> d2t("20150101")),
    TableRow("timestamp" -> d2t("20150102")),
    TableRow("timestamp" -> d2t("20150103")),
    TableRow("timestamp" -> d2t("20150201")),
    TableRow("timestamp" -> d2t("20150202")),
    TableRow("timestamp" -> d2t("20150301")))

  val distCache = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Mar")

  val expected = Seq("Jan 3", "Feb 2", "Mar 1")

  "DistCacheExample" should "work" in {
    JobTest("com.spotify.cloud.dataflow.examples.extra.DistCacheExample")
      .args("--output=out.txt")
      .input(TableRowJsonIO(DistCacheExample.EXPORTED_WIKI_TABLE), in)
      .distCache(DistCacheIO("gs://dataflow-samples/samples/misc/months.txt"), distCache)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
