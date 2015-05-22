package com.spotify.cloud.dataflow

import com.google.cloud.dataflow.sdk.coders.CoderRegistry
import com.spotify.cloud.dataflow.coders.RichCoderRegistry
import com.spotify.cloud.dataflow.values.{
  AccumulatorType, DoubleAccumulatorType, DoubleSCollectionFunctions, IntAccumulatorType, LongAccumulatorType,
  PairSCollectionFunctions, SCollection
}

import scala.language.implicitConversions
import scala.reflect.ClassTag

// A trait can be extended or mixed in
private[dataflow] trait PrivateImplicits {

  implicit protected def makeRichCoderRegistry(r: CoderRegistry): RichCoderRegistry = new RichCoderRegistry(r)

}

// A trait can be extended or mixed in
trait Implicits {

  implicit def makeDoubleSCollectionFunctions(s: SCollection[Double]): DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s)

  implicit def makeDoubleSCollectionFunctions[T](s: SCollection[T])(implicit num: Numeric[T])
  : DoubleSCollectionFunctions =
    new DoubleSCollectionFunctions(s.map(num.toDouble))

  implicit def makePairSCollectionFunctions[K: ClassTag, V: ClassTag](s: SCollection[(K, V)])
  : PairSCollectionFunctions[K, V] =
    new PairSCollectionFunctions(s)

  implicit def makeIntAccumulatorType: AccumulatorType[Int] = new IntAccumulatorType
  implicit def makeLongAccumulatorType: AccumulatorType[Long] = new LongAccumulatorType
  implicit def makeDoubleAccumulatorType: AccumulatorType[Double] = new DoubleAccumulatorType

}
