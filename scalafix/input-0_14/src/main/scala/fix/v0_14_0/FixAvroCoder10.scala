/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.avro.AvroIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec

object FixAvroCoder10 extends PipelineSpec {
  object SomeScioJob {
    def main(args: Array[String]): Unit = ???
  }

  JobTest[SomeScioJob.type]
    .output(TextIO("a")) { data => () }
    .output(AvroIO[A]("b")) { data => () }
    .output(TextIO("c")) { data => () }
}
