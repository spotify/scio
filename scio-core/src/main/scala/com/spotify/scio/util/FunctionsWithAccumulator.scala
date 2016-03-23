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

package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.{Aggregator, DoFn}
import com.spotify.scio.values.{Accumulator, AccumulatorContext}

import scala.reflect.ClassTag

abstract private class DoFnWithAccumulator[I, O](acc: Seq[Accumulator[_]]) extends DoFn[I, O] {
  private val m = {
    val b = Map.newBuilder[String, Aggregator[_, _]]
    acc.foreach(a => b += (a.name -> this.createAggregator(a.name, a.combineFn)))
    b.result()
  }

  protected def context: AccumulatorContext = new AccumulatorContext(m)
}

private[scio] object FunctionsWithAccumulator {

  def filterFn[T](f: (T, AccumulatorContext) => Boolean,
                  acc: Seq[Accumulator[_]]): DoFn[T, T] = new DoFnWithAccumulator[T, T](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      if (g(c.element(), this.context)) c.output(c.element())
    }
  }

  def flatMapFn[T, U: ClassTag](f: (T, AccumulatorContext) => TraversableOnce[U],
                                acc: Seq[Accumulator[_]])
  : DoFn[T, U] = new DoFnWithAccumulator[T, U](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      g(c.element(), this.context).foreach(c.output)
  }

  def mapFn[T, U: ClassTag](f: (T, AccumulatorContext) => U,
                            acc: Seq[Accumulator[_]])
  : DoFn[T, U] = new DoFnWithAccumulator[T, U](acc) {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      c.output(g(c.element(), this.context))
  }

}
