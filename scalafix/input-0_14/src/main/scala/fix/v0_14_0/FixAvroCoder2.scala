/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.coders.Coder
import org.apache.avro.{Schema => AvroSchema}

object FixAvroCoder2 {
  val schema: AvroSchema = ???
  Coder.avroGenericRecordCoder(schema)
  Coder.avroSpecificRecordCoder[A]
  Coder.avroSpecificFixedCoder[B]
}
