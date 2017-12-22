/*
 * Copyright 2017 Spotify AB.
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

import java.util.{Iterator => JIterator}

import com.google.common.collect.Iterators
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, TupleTag}

import scala.reflect.ClassTag

private[scio] object ArtisanJoin {

  private def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, A1, B1]
  (name: String, a: SCollection[(KEY, A)], b: SCollection[(KEY, B)])
  (leftFn: JIterator[A] => JIterator[A1], rightFn: JIterator[B] => JIterator[B1])
  : SCollection[(KEY, (A1, B1))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(name, CoGroupByKey.create())

    type DF = DoFn[KV[KEY, CoGbkResult], (KEY, (A1, B1))]
    a.context.wrap(keyed).withName(name)
      .applyTransform(ParDo.of(new DF {
        @ProcessElement
        private[util] def processElement(c: DF#ProcessContext): Unit = {
          val kv = c.element()
          val key = kv.getKey
          val result = kv.getValue
          val as = result.getAll(tagA)
          val bs = result.getAll(tagB)
          val bi = rightFn(bs.iterator())
          while (bi.hasNext) {
            val b = bi.next()
            val ai = leftFn(as.iterator())
            while (ai.hasNext) {
              val a = ai.next()
              c.output((key, (a, b)))
            }
          }
        }
      }))
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (A, B))] = cogroup(name, a, b)(identity, identity)

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                    a: SCollection[(KEY, A)],
                                                    b: SCollection[(KEY, B)])
  : SCollection[(KEY, (A, Option[B]))] = cogroup(name, a, b)(identity, toOptions)

  def right[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (Option[A], B))] = cogroup(name, a, b)(toOptions, identity)

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (Option[A], Option[B]))] = cogroup(name, a, b)(toOptions, toOptions)

  private val emptyList = java.util.Collections.singletonList(Option.empty)

  private def toOptions[A](xs: JIterator[A]): JIterator[Option[A]] =
    if (xs.hasNext) {
      Iterators.transform(xs, new com.google.common.base.Function[A, Option[A]] {
        override def apply(input: A) = Some(input)
      })
    } else {
      emptyList.iterator().asInstanceOf[JIterator[Option[A]]]
    }

}
