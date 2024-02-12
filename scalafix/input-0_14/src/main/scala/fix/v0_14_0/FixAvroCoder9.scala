/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.avro.AvroIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec

object FixAvroCoder9 extends PipelineSpec {
  object SomeScioJob {
    def main(args: Array[String]): Unit = ???
  }

  JobTest[SomeScioJob.type]
    .input(TextIO("a"), Seq())
    .input(AvroIO[A]("b"), Seq())
    .input(TextIO("c"), Seq())
}
