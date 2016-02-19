/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.testing

import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Trait for unit testing pipelines.
 */
trait PipelineSpec extends FlatSpec with Matchers with SCollectionMatcher {

  /**
   * Test pipeline components with a [[ScioContext]].
   * @param fn code that tests the components and verifies the result
   *
   * {{{
   * runWithContext { sc =>
   *   sc.parallelize(Seq(1, 2, 3)).sum should containSingleValue (6)
   * }
   * }}}
   */
  def runWithContext[T](fn: ScioContext => T): ScioResult = {
    val sc = ScioContext.forTest("PipelineTest-" + System.currentTimeMillis())
    fn(sc)
    sc.close()
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   *
   * {{{
   * runWithData(Seq(1, 2, 3)) { p =>
   *   p.sum
   * } should equal (Seq(6))
   * }}}
   */
  def runWithData[T: ClassTag, U: ClassTag](data: Iterable[T])(fn: SCollection[T] => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc => fn(sc.parallelize(data)) }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2])
  (fn: (SCollection[T1], SCollection[T2]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2))
    }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3))
    }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param data4 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, T4: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3], data4: Iterable[T4])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3], SCollection[T4]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3), sc.parallelize(data4))
    }
  }

  private def runWithLocalOutput[U](fn: ScioContext => SCollection[U]): Seq[U] = {
    val sc = ScioContext()
    val f = fn(sc).materialize
    sc.close()
    Await.result(f, Duration.Inf).value.toSeq
  }

}
