/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.util.NamedDoFn
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{TupleTag, TupleTagList}

import scala.collection.compat._ // scalafix:ok

trait SCollectionSafeSyntax {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with specialized
   * versions of flatMap.
   */
  implicit class SpecializedFlatMapSCollection[T](private val self: SCollection[T]) {

    /**
     * Latency optimized flavor of
     * [[com.spotify.scio.values.SCollection.flatMap SCollection.flatMap]], it returns a new
     * SCollection by first applying a function to all elements of this SCollection, and then
     * flattening the results. If function throws an exception, instead of retrying, faulty element
     * goes into given error side output.
     *
     * @group transform
     */
    def safeFlatMap[U: Coder](
      f: T => TraversableOnce[U]
    ): (SCollection[U], SCollection[(T, Throwable)]) = {
      val (mainTag, errorTag) = (new TupleTag[U], new TupleTag[(T, Throwable)])
      val doFn = new NamedDoFn[T, U] {
        val g = ClosureCleaner.clean(f) // defeat closure
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
          val i =
            try {
              g(c.element()).iterator
            } catch {
              case e: Throwable =>
                c.output(errorTag, (c.element(), e))
                Iterator.empty
            }
          while (i.hasNext) c.output(i.next())
        }
      }
      val tuple =
        self.applyInternal(ParDo.of(doFn).withOutputTags(mainTag, TupleTagList.of(errorTag)))
      val main = tuple
        .get(mainTag)
        .setCoder(CoderMaterializer.beam(self.context, Coder[U]))
      import self.coder
      val errorPipe =
        tuple
          .get(errorTag)
          .setCoder(CoderMaterializer.beam(self.context, Coder[(T, Throwable)]))
      (self.context.wrap(main), self.context.wrap(errorPipe))
    }
  }
}
