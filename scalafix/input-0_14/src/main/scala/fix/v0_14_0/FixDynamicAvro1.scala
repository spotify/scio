/*
rule = FixDynamicAvro
 */
package fix.v0_14_0

import com.spotify.scio.io.dynamic._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema => AvroSchema}

object FixDynamicAvro1 {
  val schema: AvroSchema = ???
  val scoll: SCollection[GenericRecord] = ???
  val dstFn: GenericRecord => String = ???
  scoll.saveAsDynamicAvroFile("", schema)(dstFn)
}
