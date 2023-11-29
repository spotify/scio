/*
rule = FixAvroSchemasPackage
 */
package fix.v0_14_0

import com.spotify.scio.schemas.Schema

object FixAvroSchemasPackage3 {
  // usage of any api where Schema is a type parameter for a type
  // that <: SpecificRecord suggests need for new import
  def x[T : Schema, U](): Unit = ???
  x[A, Int]()
}
