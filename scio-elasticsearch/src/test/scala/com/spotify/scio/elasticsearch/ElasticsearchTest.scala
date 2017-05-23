package com.spotify.scio.elasticsearch

import java.net.InetSocketAddress

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.PipelineSpec
import org.elasticsearch.action.index.IndexRequest
import org.joda.time.Duration

object ElasticsearchJob {
  val options = new ElasticsearchOptions("clusterName", Array(new InetSocketAddress(8080)))
  val data = Seq(1, 2, 3)
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)
    val shard = 2
    val toIndexRequest = (x: Int) => new IndexRequest()
    val flushInterval = Duration.standardSeconds(1)
    sc.parallelize(data)
      .saveAsElasticsearch(options, flushInterval, toIndexRequest, shard)
    sc.close()
  }
}
class ElasticsearchTest extends PipelineSpec {
  import ElasticsearchJob._
  "ElasticsearchIO" should "work" in {
    JobTest[ElasticsearchJob.type]
      .output(ElasticsearchIOTest[Int](options))(_ should containInAnyOrder(data))
      .run()
  }
}