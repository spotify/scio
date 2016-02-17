package com.spotify.scio.testing

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.values.PCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

private[scio] trait PCollectionMatcher {

  private def tryAssert(f: () => Any): Boolean = {
    try {
      f()
      true
    } catch {
      case e: Throwable => false
    }
  }

  def containInAnyOrder[T](value: Iterable[T]): Matcher[PCollection[T]] = new Matcher[PCollection[T]] {
    override def apply(left: PCollection[T]): MatchResult = {
      MatchResult(tryAssert(() => DataflowAssert.that(left).containsInAnyOrder(value.asJava)), "", "")
    }
  }

  def containSingleValue[T](value: T): Matcher[PCollection[T]] = new Matcher[PCollection[T]] {
    override def apply(left: PCollection[T]): MatchResult = {
      MatchResult(tryAssert(() => DataflowAssert.thatSingleton(left).isEqualTo(value)), "", "")
    }
  }

}
