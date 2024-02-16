
package fix.v0_14_0

import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object FixGenericAvro {

  val path: String = ???
  val numShards: Int = ???
  val schema: Schema = ???
  val suffix: String = ???

  val sc: SCollection[GenericRecord] = ???


  sc.saveAsAvroFile(path, schema, numShards)
  sc.saveAsAvroFile(path, schema, numShards, suffix)
  sc.saveAsAvroFile(path, numShards = numShards, suffix = suffix, schema = schema)

  // should not be touched
  sc.saveAsAvroFile(path, schema = schema, numShards = numShards)
  sc.saveAsAvroFile(path, schema = schema)
  sc.saveAsAvroFile(numShards = numShards, schema = schema, path = path)

}
