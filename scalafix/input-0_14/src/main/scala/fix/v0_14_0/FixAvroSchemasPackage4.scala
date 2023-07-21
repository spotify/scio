/*
rule = FixAvroSchemasPackage
 */
package fix.v0_14_0

import com.spotify.scio.schemas.Schema.fromAvroSchema
import org.apache.avro.{Schema => AvroSchema}

object FixAvroSchemasPackage4 {
  val schema: AvroSchema = ???
  fromAvroSchema(schema)
}
