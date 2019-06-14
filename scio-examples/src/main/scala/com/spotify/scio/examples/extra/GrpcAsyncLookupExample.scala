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

// Example: GRPC async lookup
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.GrpcAsyncLookupExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --host=[HOST-NAME] --port=[PORT]
// --output=gs://[BUCKET]/[PATH]/grpc-async-lookup-example"`
package com.spotify.scio.examples.extra

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.{Futures, ListenableFuture, MoreExecutors}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.proto.SimpleGrpc.TrackMetadataRequest
import com.spotify.scio.proto.TrackServiceGrpc.TrackServiceFutureStub
import com.spotify.scio.proto.{SimpleGrpc, TrackServiceGrpc}
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.{CacheSupplier, Try}
import com.spotify.scio.transforms.GuavaAsyncLookupDoFn
import io.grpc.ManagedChannelBuilder
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV

import scala.util.Success

object GrpcAsyncLookupExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.parallelize(1 to 5)
      .map("userId" + _)
      .applyTransform(ParDo.of(new GrpcAsyncLookupDoFn(args("host"), args("port").toInt)))
      .map(processResult)
      .saveAsTextFile(args("output"))
    sc.close()
    ()
  }

  def processResult(kv: KV[String, Try[String]]): String =
    kv.getValue.asScala match {
      case Success(value) => s"${kv.getKey}\t$value"
      case _              => s"${kv.getKey}\tFailed"
    }
}

class GrpcAsyncLookupDoFn(host: String, port: Int)
    extends GuavaAsyncLookupDoFn[String, String, TrackServiceFutureStub](
      10,
      new LookupCacheSupplier
    ) {

  override protected def newClient(): TrackServiceFutureStub = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    TrackServiceGrpc.newFutureStub(channel)
  }

  override def asyncLookup(
    stub: TrackServiceFutureStub,
    input: String
  ): ListenableFuture[String] = {
    val request = TrackMetadataRequest.newBuilder().setId(input).build()
    Futures.transform(
      stub.requestTrackName(request),
      new com.google.common.base.Function[SimpleGrpc.TrackMetadataReply, String] {
        override def apply(input: SimpleGrpc.TrackMetadataReply): String =
          input.getName
      },
      MoreExecutors.directExecutor()
    )
  }
}

class LookupCacheSupplier extends CacheSupplier[String, String, String] {
  override def createCache: Cache[String, String] =
    CacheBuilder
      .newBuilder()
      .maximumSize(100)
      .build[String, String]

  override def getKey(input: String): String = input
}
