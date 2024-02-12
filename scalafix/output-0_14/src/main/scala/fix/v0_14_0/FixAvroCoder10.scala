package fix.v0_14_0

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.avro._

object FixAvroCoder10 extends PipelineSpec {
  object SomeScioJob {
    def main(args: Array[String]): Unit = ???
  }

  JobTest[SomeScioJob.type]
    .output(TextIO("a")) { data => () }
    .output(AvroIO[A]("b")) { data => () }
    .output(TextIO("c")) { data => () }
}
