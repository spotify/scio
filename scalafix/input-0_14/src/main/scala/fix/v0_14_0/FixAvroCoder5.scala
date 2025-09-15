/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.coders.Coder
import org.apache.avro.generic.GenericRecord

object FixAvroCoder5 {

  implicit val c = Coder.avroGenericRecordCoder

  def foo[T : Coder]() = ???
  val r = foo[GenericRecord]
}
