/*
 * Copyright 2019 Spotify AB.
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

// Example: Http AsyncLookup
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.HttpAsyncLookupExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --url="http://[HOST-URL]:[PORT]/PATH
// --output=gs://[BUCKET]/[PATH]/http_async_lookup_example"`
package com.spotify.scio.examples.extra

import java.io.IOException

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent._
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.examples.extra.HttpAsyncLookupExample.Account
import com.spotify.scio.transforms.AsyncLookupDoFn
import com.spotify.scio.transforms.AsyncLookupDoFn.{CacheSupplier, Try}
import com.squareup.okhttp.{Callback, OkHttpClient, Request, Response}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV

import scala.util.Success

object HttpAsyncLookupExample {

  case class Account(id: String, amount: Double)

  lazy val okHttpClient = new OkHttpClient

  implicit def coderTry: Coder[Try[String]] = Coder.kryo[Try[String]]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(1 to 5)
      .map(e => Account(e.toString, e.toDouble))
      .applyTransform(ParDo.of(new HttpAsyncLookup(args("url"))))
      .map(processResult)
      .saveAsTextFile(args("output"))
    sc.close()
    ()
  }

  def processResult(kv: KV[Account, AsyncLookupDoFn.Try[String]]): String =
    kv.getValue.asScala match {
      case Success(value) => s"${kv.getKey.id}\t${kv.getKey.amount}\t$value"
      case _              => s"${kv.getKey.id}\t${kv.getKey.amount}\tFailed"
    }
}

class HttpAsyncLookup(url: String)
    extends AsyncLookupDoFn[Account, String, AsyncHttpClient](100, new LookupCacheSupplier) {

  override def asyncLookup(client: AsyncHttpClient, input: Account): ListenableFuture[String] = {
    Futures.transform(
      client.get(url + "?id=" + input.id),
      new com.google.common.base.Function[Option[String], String] {
        override def apply(input: Option[String]): String =
          input.get
      },
      MoreExecutors.directExecutor
    )
  }

  override protected def newClient(): AsyncHttpClient = new AsyncHttpClient
}

class LookupCacheSupplier extends CacheSupplier[Account, String, String] {
  override def createCache(): Cache[String, String] =
    CacheBuilder
      .newBuilder()
      .maximumSize(100)
      .build[String, String]

  override def getKey(input: Account): String = input.id
}

class AsyncHttpClient {

  def get(url: String): ListenableFuture[Option[String]] = {
    val request = new Request.Builder()
      .url(url)
      .get
      .build

    val result: SettableFuture[Option[String]] = SettableFuture.create()
    HttpAsyncLookupExample.okHttpClient
      .newCall(request)
      .enqueue(new Callback() {
        def onFailure(request: Request, exception: IOException): Unit = {
          result.setException(exception)
          ()
        }

        def onResponse(response: Response): Unit = {
          if (response.isSuccessful) {
            result.set(Option(response.body.string))
            ()
          } else {
            result.set(Option("Default Value"))
            ()
          }
        }
      })

    result
  }
}
