package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.testing._
import org.joda.time.format.DateTimeFormat


class HadoopDistCacheTest extends PipelineSpec {
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

  "HadoopDistCacheExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.HadoopDistCacheExample.type]
      .args("--output=out.txt")
      .input(TableRowJsonIO(ExampleData.EXPORTED_WIKI_TABLE), in)
      .distCache(DistCacheIO("hdfs://dataflow-samples/samples/misc/months.txt"), distCache)
      .output(TextIO("out.txt"))(_ should containInAnyOrder(expected))
      .run()
  }
}
