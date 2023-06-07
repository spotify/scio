package fix.v0_8_0

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

class SpecializedScioIO extends ScioIO[(String, Int)] {
  override type ReadP = String
  override type WriteP = Nothing
  override val tapT: TapT[(String, Int)] = TapOf[(String, Int)]

  override def read(sc: ScioContext,
                    endpoint: String): SCollection[(String, Int)] = ???

  override protected def write(data: SCollection[(String, Int)],
                               params: WriteP): Tap[tapT.T] =
    throw new IllegalStateException("🤡")

  override def tap(params: ReadP): Tap[tapT.T] = ???
}

class OtherScioIO extends ScioIO[Integer] {
  override type ReadP = String
  override type WriteP = Nothing
  override val tapT: TapT[Integer] = TapOf[Integer]

  override def read(sc: ScioContext, endpoint: String): SCollection[Integer] =
    ???

  override def write(data: SCollection[Integer],
                     params: WriteP): Tap[tapT.T] =
    throw new IllegalStateException("🤡")

  override def tap(params: ReadP): Tap[tapT.T] = ???
}
