package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.values.{TupleTagList, TupleTag, PCollection}
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithSideOutput

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class SCollectionWithSideOutput[T] private[values] (val internal: PCollection[T],
                                                    sides: Iterable[SideOutput[_]])
                                                   (implicit private[values] val context: DataflowContext,
                                                    protected val ct: ClassTag[T])
  extends PCollectionWrapper[T] {

  private val sideTags = TupleTagList.of(sides.map(_.tupleTag).toList.asJava)

  def flatMap[U: ClassTag](f: (T, SideOutputContext[T]) => TraversableOnce[U]): (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(ParDo.withOutputTags(mainTag, sideTags).of(FunctionsWithSideOutput.flatMapFn(f)))

    val main = tuple.get(mainTag).setCoder(this.getCoder[U])
    (SCollection(main), new SideOutputCollections(tuple))
  }

  def map[U: ClassTag](f: (T, SideOutputContext[T]) => U): (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(ParDo.withOutputTags(mainTag, sideTags).of(FunctionsWithSideOutput.mapFn(f)))

    val main = tuple.get(mainTag).setCoder(this.getCoder[U])
    (SCollection(main), new SideOutputCollections(tuple))
  }

}
