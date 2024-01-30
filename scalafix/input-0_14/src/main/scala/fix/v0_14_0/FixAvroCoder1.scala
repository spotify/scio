/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.coders.Coder.{avroGenericRecordCoder, avroSpecificFixedCoder, avroSpecificRecordCoder}
import org.apache.avro.{Schema => AvroSchema}

object FixAvroCoder1 {
  val schema: AvroSchema = ???
  avroGenericRecordCoder
  avroGenericRecordCoder(schema)
  avroSpecificRecordCoder[A]
  avroSpecificFixedCoder[B]
}
