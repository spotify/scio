package com.spotify.cloud.dataflow.testing

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.values.PCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

private[dataflow] trait PCollectionMatchers {

  private def tryAssert(f: () => Any): Boolean = {
    try {
      f()
      true
    } catch {
      case e: Throwable => false
    }
  }

  class InAnyOrderMatcher[T](value: Iterable[T]) extends Matcher[PCollection[T]] {
    override def apply(left: PCollection[T]): MatchResult = {
      MatchResult(tryAssert(() => DataflowAssert.that(left).containsInAnyOrder(value.asJava)), "", "")
    }
  }

  class InOrderMatcher[T](value: Iterable[T]) extends Matcher[PCollection[T]] {
    override def apply(left: PCollection[T]): MatchResult = {
      MatchResult(tryAssert(() => DataflowAssert.that(left).containsInAnyOrder(value.asJava)), "", "")
    }
  }

  def containInAnyOrder[T](value: T*): Matcher[PCollection[T]] = new InAnyOrderMatcher(value)

  def equalInAnyOrder[T](value: Iterable[T]): Matcher[PCollection[T]] = new InAnyOrderMatcher(value)

  def containSingleValue[T](value: T): Matcher[PCollection[T]] = new Matcher[PCollection[T]] {
    override def apply(left: PCollection[T]): MatchResult = {
      MatchResult(tryAssert(() => DataflowAssert.thatSingleton(left).isEqualTo(value)), "", "")
    }
  }

}
