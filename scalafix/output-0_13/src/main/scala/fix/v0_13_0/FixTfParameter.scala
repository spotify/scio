package fix.v0_13_0

import com.spotify.scio.values.SCollection
import com.spotify.scio.tensorflow._
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow._

object FixTfParameter {
  case class A()
  case class B()
  case class C()

  def toTensors(a: A): Map[String, Tensor] = ???
  def fromTensors(a: A, tensors: Map[String, Tensor]): B = ???

  val elements: SCollection[A] = ???
  val options: TensorFlowModel.Options = ???
  val fetchOpts: Seq[String] = ???

  val result: SCollection[B] = elements.predict[B]("gs://model-path", fetchOpts, options)(toTensors)(fromTensors)
  val b: SCollection[B] = elements.predictWithSigDef[B]("gs://model-path", options)(toTensors)(fromTensors _)
}
