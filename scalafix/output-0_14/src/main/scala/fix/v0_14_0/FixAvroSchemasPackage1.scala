
package fix.v0_14_0

import com.spotify.scio.schemas.Schema
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.specific.SpecificRecord
import com.spotify.scio.avro.schemas._

class A extends SpecificRecord {
  override def put(i: Int, v: Any): Unit = ???
  override def get(i: Int): AnyRef = ???
  override def getSchema: AvroSchema = ???
}

object FixAvroSchemasPackage1 {
  val schema: AvroSchema = ???
  // direct usage should be converted
  fromAvroSchema(schema)
}
