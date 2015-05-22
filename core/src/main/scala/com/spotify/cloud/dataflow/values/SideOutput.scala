package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.{PCollectionTuple, TupleTag}
import com.spotify.cloud.dataflow.{DataflowContext, Implicits}
import org.joda.time.Instant

import scala.reflect.ClassTag

trait SideOutput[T] extends Serializable {
  private[values] val tupleTag: TupleTag[T]
}

object SideOutput {
  def apply[T](): SideOutput[T] = new SideOutput[T] {
    override private[values] val tupleTag: TupleTag[T] = new TupleTag[T]()
  }
}

class SideOutputContext[T] private[dataflow] (private val context: DoFn[T, AnyRef]#ProcessContext) {
  def output[S](sideOutput: SideOutput[S], output: S, timestamp: Instant = null): SideOutputContext[T] = {
    if (timestamp == null) {
      context.sideOutput(sideOutput.tupleTag, output)
    } else {
      context.sideOutputWithTimestamp(sideOutput.tupleTag, output, timestamp)
    }
    this
  }
}

class SideOutputCollections private[values] (private val tuple: PCollectionTuple)
                                            (implicit val context: DataflowContext) extends Implicits {
  def apply[T: ClassTag](sideOutput: SideOutput[T]): SCollection[T] = {
    val r = tuple.getPipeline.getCoderRegistry
    val o = tuple.get(sideOutput.tupleTag).setCoder(r.getScalaCoder[T])
    SCollection(o)
  }
}
