package com.spotify.scio.examples.extra

import com.spotify.scio.elasticsearch.{ElasticsearchIO, ElasticsearchOptions}
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.apache.http.HttpHost

class ElasticsearchMinimalExampleTest extends PipelineSpec {

  private val inData = Seq(
    "Lorem ipsum",
    "dolor sit amet",
    "Lorem ipsum"
  )

  private val outData = Seq(
    "Lorem" -> 2L,
    "ipsum" -> 2L,
    "dolor" -> 1L,
    "sit" -> 1L,
    "amet" -> 1L
  )

  private val esOptions = ElasticsearchOptions(
    Seq(new HttpHost("host", 1234))
  )

  "ElasticsearchMinimalExample" should "work" in {
    JobTest[ElasticsearchMinimalExample.type]
      .args(
        "--input=in.txt",
        "--esHost=host",
        "--esPort=1234"
      )
      .input(TextIO("in.txt"), inData)
      .output(ElasticsearchIO[(String, Long)](esOptions))(_ should containInAnyOrder(outData))
      .run()
  }

}
