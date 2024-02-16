/*
rule = FixAvroSchemasPackage
 */
package fix.v0_14_0

import com.spotify.scio.schemas.Schema
import org.apache.avro.{Schema => AvroSchema}

object FixAvroSchemasPackage1 {
  val schema: AvroSchema = ???
  // direct usage should be converted
  Schema.fromAvroSchema(schema)
}
