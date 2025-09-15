/*
rule = FixSMBCharSequenceKey
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.smb._
import com.spotify.scio.smb.util.SMBMultiJoin
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.values.TupleTag

object FixSMBCharSequenceKey {
  implicit val c: Coder[GenericRecord] = ???
  val schema: Schema = ???
  val sc = ScioContext()

  def read: AvroSortedBucketIO.Read[GenericRecord] = AvroSortedBucketIO.read(new TupleTag[GenericRecord](), schema)

  sc.sortMergeJoin(classOf[CharSequence], read, read)

  sc.sortMergeTransform(classOf[CharSequence], read, read)

  sc.sortMergeGroupByKey(classOf[CharSequence], read)

  sc.sortMergeCoGroup(classOf[CharSequence], read, read)

  SMBMultiJoin(sc).sortMergeCoGroup(classOf[CharSequence], read, read, read, read, read, TargetParallelism.max())

  SMBMultiJoin(sc).sortMergeTransform(classOf[CharSequence], read, read, read, read, read, TargetParallelism.max())

  // Should not be changed
  sc.sortMergeJoin(classOf[Integer], read, read)

  SMBMultiJoin(sc).sortMergeCoGroup(classOf[Integer], read, read, read, read, read, TargetParallelism.max())
}
