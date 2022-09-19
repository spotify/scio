/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.redis

import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.DoFn
import redis.clients.jedis.{Jedis, Response, Transaction}

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.display.DisplayData.Builder
import org.apache.beam.sdk.transforms.display.DisplayData
import org.joda.time.Instant

import scala.concurrent.{Future, Promise}
import scala.util._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

abstract class RedisDoFn[I, O](
  connectionConfig: RedisConnectionConfiguration,
  batchSize: Int
) extends DoFn[I, O] {

  @transient private var jedis: Jedis = _
  @transient private var transaction: Transaction = _
  private val results: ConcurrentLinkedQueue[Future[Result]] = new ConcurrentLinkedQueue()
  private val requests: ConcurrentLinkedQueue[(List[Response[_]], Promise[List[_]])] =
    new ConcurrentLinkedQueue()
  private var batchCount = 0
  private val client = new Client()

  private case class Result(input: I, output: O, ts: Instant, w: BoundedWindow)

  final class Client extends Serializable {
    type Request = Transaction => List[Response[_]]

    def request(request: Request): Future[List[_]] = {
      val promise = Promise[List[_]]()
      requests.add((request(transaction), promise))
      promise.future
    }
  }

  def this(opts: RedisConnectionOptions, batchSize: Int) =
    this(RedisConnectionOptions.toConnectionConfig(opts), batchSize)

  def executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  private def flush(fn: Result => Unit): Unit = {
    transaction.exec

    implicit val ec = executionContext
    val iter = requests.iterator()
    while (iter.hasNext()) {
      val (rsp, promise) = iter.next()
      promise.success(rsp.flatMap(r => Option(r.get())))
    }

    val future = Future.sequence(results.asScala).andThen {
      case Success(value) =>
        val iter = value.iterator
        while (iter.hasNext) {
          fn(iter.next())
        }
      case Failure(_) => ()
    }

    Await.result(future, Duration.Inf)

    results.clear()
    requests.clear()
  }

  def request(value: I, client: Client)(implicit ec: ExecutionContext): Future[O]

  @Setup
  def setup(): Unit =
    jedis = connectionConfig.connect

  @StartBundle
  def startBundle(): Unit = {
    transaction = jedis.multi
    batchCount = 0
  }

  @ProcessElement
  def processElement(
    @Element element: I,
    @Timestamp timestamp: Instant,
    o: OutputReceiver[O],
    window: BoundedWindow
  ): Unit = {
    implicit val ec = executionContext
    val result = request(element, client).map { r =>
      Result(element, r, timestamp, window)
    }
    results.add(result)

    batchCount += 1
    if (batchCount >= batchSize) {
      flush(r => o.output(r.output))
      transaction.multi
      batchCount = 0
    }
  }

  @FinishBundle
  def finishBundle(c: FinishBundleContext): Unit = {
    flush(r => c.output(r.output, r.ts, r.w))
    batchCount = 0
  }

  @Teardown def teardown(): Unit =
    jedis.close()

  override def populateDisplayData(builder: Builder): Unit = {
    connectionConfig.populateDisplayData(builder)
    builder.add(DisplayData.item("batch-size", batchSize: java.lang.Integer))
    ()
  }

}
