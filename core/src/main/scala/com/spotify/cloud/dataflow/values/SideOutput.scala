package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.{PCollectionTuple, TupleTag}
import com.spotify.cloud.dataflow.{DataflowContext, PrivateImplicits}
import org.joda.time.Instant

import scala.reflect.ClassTag

/** Encapsulate a side output for a transform. */
trait SideOutput[T] extends Serializable {
  private[values] val tupleTag: TupleTag[T]
}

/** Companion object for [[SideOutput]]. */
object SideOutput {
  /** Create a new [[SideOutput]] instance. */
  def apply[T](): SideOutput[T] = new SideOutput[T] {
    override private[values] val tupleTag: TupleTag[T] = new TupleTag[T]()
  }
}

/** Encapsulate context of one or more [[SideOutput]]s in an [[SCollectionWithSideOutput]]. */
class SideOutputContext[T] private[dataflow] (private val context: DoFn[T, AnyRef]#ProcessContext) {
  /** Write a value to a given [[SideOutput]]. */
  def output[S](sideOutput: SideOutput[S], output: S, timestamp: Instant = null): SideOutputContext[T] = {
    if (timestamp == null) {
      context.sideOutput(sideOutput.tupleTag, output)
    } else {
      context.sideOutputWithTimestamp(sideOutput.tupleTag, output, timestamp)
    }
    this
  }
}

/** Encapsulate output of one or more [[SideOutput]]s in an [[SCollectionWithSideOutput]]. */
class SideOutputCollections private[values] (private val tuple: PCollectionTuple)
                                            (implicit context: DataflowContext) extends PrivateImplicits {
  /** Extract the [[SCollection]] of a given [[SideOutput]]. */
  def apply[T: ClassTag](sideOutput: SideOutput[T]): SCollection[T] = {
    val r = tuple.getPipeline.getCoderRegistry
    val o = tuple.get(sideOutput.tupleTag).setCoder(r.getScalaCoder[T])
    SCollection(o)
  }
}
