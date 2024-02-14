package com.spotify.scio.avro.magnolify

import com.spotify.scio.avro.AvroIO
import com.spotify.scio.testing.ScioIOSpec

object MagnolifyAvroIOTest {
  case class Record(i: Int, s: String, r: List[String])
}

class MagnolifyAvroIOTest extends ScioIOSpec {
  import MagnolifyAvroIOTest._

  "TypedMagnolifyAvroIO" should "work with typed Avro" in {
    val xs = (1 to 100).map(x => Record(x, x.toString, (1 to x).map(_.toString).toList))
    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO[Record])(_.typedAvroFile[Record](_))(_.saveAsTypedAvroFile(_))
  }
}
