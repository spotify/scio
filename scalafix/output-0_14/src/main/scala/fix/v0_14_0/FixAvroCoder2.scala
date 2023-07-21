
package fix.v0_14_0

import com.spotify.scio.coders.Coder
import org.apache.avro.{Schema => AvroSchema}
import com.spotify.scio.avro._

object FixAvroCoder2 {
  val schema: AvroSchema = ???
  avroGenericRecordCoder(schema)
  avroSpecificRecordCoder[A]
  avroSpecificFixedCoder[B]
}
