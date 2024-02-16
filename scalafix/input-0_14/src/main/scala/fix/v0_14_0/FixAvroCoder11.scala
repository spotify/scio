/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.smb._
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

object FixAvroCoder11 {
  val read = AvroSortedBucketIO.read(new TupleTag[A], classOf[A])
  val sc = ScioContext()

  sc.sortMergeGroupByKey(classOf[String], read)
}
