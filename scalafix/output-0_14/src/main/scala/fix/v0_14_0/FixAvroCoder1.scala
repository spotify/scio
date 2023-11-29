
package fix.v0_14_0

import org.apache.avro.specific.SpecificFixed
import org.apache.avro.{Schema => AvroSchema}
import java.io.{ObjectInput, ObjectOutput}
import com.spotify.scio.avro._

class B extends SpecificFixed {
  override def getSchema: AvroSchema = ???
  override def writeExternal(out: ObjectOutput): Unit = ???
  override def readExternal(in: ObjectInput): Unit = ???
}

object FixAvroCoder1 {
  val schema: AvroSchema = ???
  avroGenericRecordCoder(schema)
  avroSpecificRecordCoder[A]
  avroSpecificFixedCoder[B]
}

