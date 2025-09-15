/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT, TextIO}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection

object FixAvroCoder9 extends PipelineSpec {
  object SomeScioJob {
    def main(args: Array[String]): Unit = ???
  }

  case class SomeScioIO[T](path: String) extends ScioIO[T] {
    override type ReadP = Unit
    override type WriteP = Unit
    override val tapT: TapT[T] = TapOf[T]
    override protected def read(sc: ScioContext, params: Unit): SCollection[T] = ???
    override protected def write(data: SCollection[T], params: Unit): Tap[tapT.T] = ???
    override def tap(read: Unit): Tap[tapT.T] = ???
  }

  JobTest[SomeScioJob.type]
    .input(TextIO("a"), Seq())
    .input(SomeScioIO[A]("b"), Seq())
    .input(TextIO("c"), Seq())
}
