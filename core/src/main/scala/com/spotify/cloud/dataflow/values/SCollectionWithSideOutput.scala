package com.spotify.cloud.dataflow.values

import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.values.{TupleTagList, TupleTag, PCollection}
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.util.FunctionsWithSideOutput

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * An enhanced SCollection that provides access to one or more [[SideOutput]]s for some transforms.
 * [[SideOutput]]s are accessed via the additional [[SideOutputContext]] argument.
 * [[SCollection]]s of the [[SideOutput]]s are accessed via the additional
 * [[SideOutputCollections]] return value.
 */
class SCollectionWithSideOutput[T: ClassTag] private[values] (val internal: PCollection[T],
                                                              private[values] val context: DataflowContext,
                                                              sides: Iterable[SideOutput[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  private val sideTags = TupleTagList.of(sides.map(_.tupleTag).toList.asJava)

  /**
   * [[SCollection.flatMap]] with an additional SideOutputContext argument and additional
   * SideOutputCollections return value.
   */
  def flatMap[U: ClassTag](f: (T, SideOutputContext[T]) => TraversableOnce[U])
  : (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(ParDo.withOutputTags(mainTag, sideTags).of(FunctionsWithSideOutput.flatMapFn(f)))

    val main = tuple.get(mainTag).setCoder(this.getCoder[U])
    (context.wrap(main), new SideOutputCollections(tuple, context))
  }

  /**
   * [[SCollection.map]] with an additional SideOutputContext argument and additional
   * SideOutputCollections return value.
   */
  def map[U: ClassTag](f: (T, SideOutputContext[T]) => U): (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(ParDo.withOutputTags(mainTag, sideTags).of(FunctionsWithSideOutput.mapFn(f)))

    val main = tuple.get(mainTag).setCoder(this.getCoder[U])
    (context.wrap(main), new SideOutputCollections(tuple, context))
  }

}
