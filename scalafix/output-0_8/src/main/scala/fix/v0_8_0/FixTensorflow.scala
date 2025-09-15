package fix.v0_8_0

import com.spotify.scio.tensorflow._
import com.spotify.scio.values.SCollection
import org.tensorflow.example.Example

object FixTensorflowExample {
  def getReminders(sc: SCollection[Example], input: String) = {
    sc.map(identity).saveAsTfRecordFile("path")
  }
}
