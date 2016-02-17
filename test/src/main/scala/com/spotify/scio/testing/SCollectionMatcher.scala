package com.spotify.scio.testing

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.spotify.scio.values.SCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

private[scio] trait SCollectionMatcher {

  private def tryAssert(f: () => Any): Boolean = {
    try {
      f()
      true
    } catch {
      case e: Throwable => false
    }
  }

  def containInAnyOrder[T](value: Iterable[T]): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).containsInAnyOrder(value.asJava)), "", "")
  }

  def containSingleValue[T](value: T): Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.thatSingleton(left.internal).isEqualTo(value)), "", "")
  }

}
