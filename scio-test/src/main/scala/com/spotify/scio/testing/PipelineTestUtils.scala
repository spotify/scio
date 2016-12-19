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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

import com.google.api.client.util.ByteStreams
import com.google.cloud.dataflow.sdk.util.MimeTypes
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
import com.spotify.scio.values.{DistCache, SCollection}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Trait with utility methods for unit testing pipelines. */
trait PipelineTestUtils { this: ContextProvider =>

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
    val sc = this.context
    fn(sc)
    sc.close()
  }

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as an [[SCollection]] and the result [[SCollection]] from `fn`
   * is extracted and to be verified.
   *
   * @param data input data
   * @param fn transform to be tested
   * @return output data
   *
   * {{{
   * runWithData(Seq(1, 2, 3)) { p =>
   *   p.sum
   * } should equal (Seq(6))
   * }}}
   */
  def runWithData[T: ClassTag, U: ClassTag](data: Iterable[T])
                                           (fn: SCollection[T] => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc => fn(sc.parallelize(data)) }
  }

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as [[SCollection]]s and the result [[SCollection]] from `fn` is
   * extracted and to be verified.
   *
   * @param data1 input data
   * @param data2 input data
   * @param fn transform to be tested
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
   *
   * Input data is passed to `fn` as [[SCollection]]s and the result [[SCollection]] from `fn` is
   * extracted and to be verified.
   *
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param fn transform to be tested
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
   *
   * Input data is passed to `fn` as [[SCollection]]s and the result [[SCollection]] from `fn` is
   * extracted and to be verified.
   *
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param data4 input data
   * @param fn transform to be tested
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, T4: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3], data4: Iterable[T4])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3], SCollection[T4]) => SCollection[U])
  : Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3), sc.parallelize(data4))
    }
  }

  private def runWithLocalOutput[U](fn: ScioContext => SCollection[U]): Seq[U] = {
    val sc = ScioContext()
    val f = fn(sc).materialize
    sc.close()
    f.waitForResult().value.toSeq
  }

}

trait PipelineUnitTestUtils extends PipelineTestUtils with UnitTestContextProvider

/** Internal trait for integration testing. */
private[scio] trait PipelineITUtils extends PipelineTestUtils with IntegrationTestContextProvider {

  private val gcsUtil = new GcsUtilFactory().create(this.context.options)

  /**
   * Test pipeline components with a [[ScioContext]] by supplying data for a distributed cache.
   * @param data the data for the distributed cache
   * @param path the GCS file where the cache will be written
   * @param fn code that tests the components and verifies the result
   *
   * {{{
   * runWithDistCache(Seq("a", "b"), "gs://<bucket>/<file>") { sc =>
   *   val cache = sc.distCache("gs://<bucket>/<file>") (initFn) // Can use in test
   *   sc.parallelize(Seq(1, 2, 3)).sum should containSingleValue (6)
   * }
   * }}}
   */
  def runWithDistCache[T1: ClassTag, T2: ClassTag](data: Iterable[T1], path: String)
                                                  (fn: ScioContext => T2)
  : ScioResult = {
    try {
      val channel = gcsUtil.create(GcsPath.fromUri(path), MimeTypes.BINARY)
      channel.write(ByteBuffer.wrap(data.mkString("\n").getBytes))
      channel.close()
      runWithContext(fn)
    }
    finally {
      val gcsPath = GcsPath.fromUri(path)
      if (gcsUtil.bucketExists(gcsPath)) {
        gcsUtil.remove(Seq(path).asJava)
      }
    }
  }

}
