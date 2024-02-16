package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.smb._
import org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag
import com.spotify.scio.avro._

object FixAvroCoder12 {
  val sc = ScioContext()

  sc.sortMergeGroupByKey(classOf[String], ParquetAvroSortedBucketIO.read(new TupleTag[A], classOf[A]))
}
