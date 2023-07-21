package fix.v0_14_0

import com.spotify.scio.schemas.Schema
import com.spotify.scio.avro.schemas._

object FixAvroSchemasPackage2 {
  // usage of Schema[T <: SpecificRecord] suggests need for new import
  val x = Schema[A]
}
