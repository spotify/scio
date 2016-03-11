/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

  val beEmpty = new Matcher[SCollection[_]] {
    override def apply(left: SCollection[_]): MatchResult =
      MatchResult(tryAssert(() => DataflowAssert.that(left.internal).empty()), "", "")
  }

}
