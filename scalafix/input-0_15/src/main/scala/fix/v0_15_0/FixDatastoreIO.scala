/*
rule = FixDatastoreIO
*/
package fix.v0_15_0

import com.spotify.scio.datastore.DatastoreIO

object FixDatastoreIO {
  val io1 = DatastoreIO("my-project-id")
}
