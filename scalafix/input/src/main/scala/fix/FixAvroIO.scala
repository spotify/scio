/*
rule = FixAvroIO
*/
package fix
package v0_7_0

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import com.spotify.scio.avro._

case class InputClass(s: String, i: Int)
case class OutputClass(result: String)

object TestJob

class ValidationJobTest extends PipelineSpec {
  val inputs: List[InputClass] = (1 to 10).toList.map{ i => InputClass(s"s$i", i) }
  val inputs2 = (1 to 10).zip(inputs).toMap
  val inputs3 = inputs2.values
  val expected = List(OutputClass("result"))

  "TestJob" should "run" in {
    JobTest[TestJob.type]
      .input(AvroIO("current"), inputs)
      .input(AvroIO("reference"), inputs2.values)
      .input(AvroIO("reference2"), inputs3)
      .input(AvroIO[InputClass]("donttouch"), inputs)
      .output[OutputClass](AvroIO("foo"))(_ should containInAnyOrder(expected))
      .run()
  }
}