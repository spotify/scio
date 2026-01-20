package fix.v0_15_0

import com.spotify.scio.datastore.DatastoreIO
import com.google.datastore.v1.Entity

object FixDatastoreIO {
  val io1 = DatastoreIO[Entity]("my-project-id")
}
