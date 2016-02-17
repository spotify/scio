package com.spotify.scio.examples.extra

import com.spotify.scio.testing._

class TypedBigQueryTornadoesTest extends PipelineSpec {

  import com.spotify.scio.examples.cookbook.BigQueryTornadoesTest._

  "TypedBigQueryTornadoes" should "work" in {
    JobTest("com.spotify.scio.examples.extra.TypedBigQueryTornadoes")
      .args("--output=dataset.table")
      .input(BigQueryIO("SELECT tornado, month FROM [publicdata:samples.gsod]"), input)
      .output(BigQueryIO("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
