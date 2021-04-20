/*
rule = AddMissingImports
*/
package fix
package v0_7_0

import com.google.protobuf.Message
import com.spotify.scio._
import scala.reflect.ClassTag
import com.spotify.scio.values.SCollection

object AddMissingImports {

  def computeAndSaveDay[M <: Message : ClassTag](sc: ScioContext): Unit = {
    sc.protobufFile[M]("input")
      .saveAsProtobufFile("output")

    sc.close()
    ()
  }
}
