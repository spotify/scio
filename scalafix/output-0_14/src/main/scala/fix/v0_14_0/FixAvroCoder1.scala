
package fix.v0_14_0

import org.apache.avro.{Schema => AvroSchema}
import com.spotify.scio.avro._

object FixAvroCoder1 {
  val schema: AvroSchema = ???
  avroGenericRecordCoder
  avroGenericRecordCoder(schema)
  avroSpecificRecordCoder[A]
  avroSpecificFixedCoder[B]
}
