/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.coders.Coder.{avroGenericRecordCoder, avroSpecificFixedCoder, avroSpecificRecordCoder}
import org.apache.avro.specific.SpecificFixed
import org.apache.avro.{Schema => AvroSchema}
import java.io.{ObjectInput, ObjectOutput}

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
