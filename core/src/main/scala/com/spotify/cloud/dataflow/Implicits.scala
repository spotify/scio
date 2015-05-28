package com.spotify.cloud.dataflow

import com.google.cloud.dataflow.sdk.coders.CoderRegistry
import com.spotify.cloud.dataflow.coders.RichCoderRegistry
import com.spotify.cloud.dataflow.values.{
  AccumulatorType, DoubleAccumulatorType, IntAccumulatorType, LongAccumulatorType
}

import scala.language.implicitConversions

// A trait can be extended or mixed in
private[dataflow] trait PrivateImplicits {
  implicit protected def makeRichCoderRegistry(r: CoderRegistry): RichCoderRegistry = new RichCoderRegistry(r)
}

// A trait can be extended or mixed in
trait Implicits {

  implicit def makeIntAccumulatorType: AccumulatorType[Int] = new IntAccumulatorType
  implicit def makeLongAccumulatorType: AccumulatorType[Long] = new LongAccumulatorType
  implicit def makeDoubleAccumulatorType: AccumulatorType[Double] = new DoubleAccumulatorType

}
