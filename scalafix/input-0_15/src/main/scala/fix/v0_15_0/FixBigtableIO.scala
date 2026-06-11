/*
rule = FixBigtableIO
*/
package fix.v0_15_0

import com.google.bigtable.v2.RowFilter
import com.spotify.scio.ScioContext
import com.spotify.scio.bigtable._
import org.apache.beam.sdk.io.range.ByteKeyRange

object FixBigtableIO {
  val sc: ScioContext = ???
  val keyRanges: Seq[ByteKeyRange] = ???
  val rowFilter: RowFilter = ???
  val keyRange: ByteKeyRange = ???

  // 5-arg call with Seq[ByteKeyRange] - should migrate to BTOptions overload
  sc.bigtable("project", "instance", "table", keyRanges, rowFilter)

  // 4-arg call with Seq[ByteKeyRange] - should migrate to BTOptions overload
  sc.bigtable("project", "instance", "table", keyRanges)

  // 5-arg call with ByteKeyRange - should NOT be fixed
  sc.bigtable("project", "instance", "table", keyRange, rowFilter)

  // 3-arg call - should NOT be fixed
  sc.bigtable("project", "instance", "table")

  // 6-arg call already explicit - should NOT be fixed
  sc.bigtable("project", "instance", "table", keyRanges, rowFilter, None)
}
