/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.grpc

import com.google.common.util.concurrent.ListenableFuture
import com.spotify.scio.coders.Coder
import com.spotify.scio.grpc.GrpcLookupFunctions.StreamObservableFuture
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.{CacheSupplier, NoOpCacheSupplier}
import com.spotify.scio.transforms.JavaAsyncConverters._
import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions.kvToTuple
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import io.grpc.Channel
import io.grpc.stub.{AbstractFutureStub, AbstractStub, StreamObserver}

import java.lang.{Iterable => JIterable}

import scala.util.Try
import scala.jdk.CollectionConverters._

class GrpcSCollectionOps[Request](private val self: SCollection[Request]) extends AnyVal {

  def grpcLookup[Response: Coder, Client <: AbstractFutureStub[Client]](
    channelSupplier: () => Channel,
    clientFactory: Channel => Client,
    maxPendingRequests: Int,
    cacheSupplier: CacheSupplier[Request, Response] = new NoOpCacheSupplier[Request, Response]()
  )(f: Client => Request => ListenableFuture[Response]): SCollection[(Request, Try[Response])] =
    self.transform { in =>
      import self.coder
      val cs = ClosureCleaner.clean(channelSupplier)
      val cf = Functions.serializableFn(clientFactory)
      val lfn = Functions.serializableBiFn[Client, Request, ListenableFuture[Response]] {
        (client, request) => f(client)(request)
      }
      in.parDo(
        GrpcDoFn
          .newBuilder[Request, Response, Client]()
          .withChannelSupplier(() => cs())
          .withNewClientFn(cf)
          .withLookupFn(lfn)
          .withMaxPendingRequests(maxPendingRequests)
          .withCacheSupplier(cacheSupplier)
          .build()
      ).map(kvToTuple)
        .mapValues(_.asScala)
    }

  def grpcLookupStream[Response: Coder, Client <: AbstractStub[Client]](
    channelSupplier: () => Channel,
    clientFactory: Channel => Client,
    maxPendingRequests: Int,
    cacheSupplier: CacheSupplier[Request, JIterable[Response]] =
      new NoOpCacheSupplier[Request, JIterable[Response]]()
  )(
    f: Client => (Request, StreamObserver[Response]) => Unit
  ): SCollection[(Request, Try[Iterable[Response]])] = self.transform { in =>
    import self.coder
    val cs = ClosureCleaner.clean(channelSupplier)
    val cf = Functions.serializableFn(clientFactory)
    val lfn = Functions.serializableBiFn[Client, Request, ListenableFuture[JIterable[Response]]] {
      (client, request) =>
        val observer = new StreamObservableFuture[Response]()
        f(client)(request, observer)
        observer
    }
    in.parDo(
      GrpcDoFn
        .newBuilder[Request, JIterable[Response], Client]()
        .withChannelSupplier(() => cs())
        .withNewClientFn(cf)
        .withLookupFn(lfn)
        .withMaxPendingRequests(maxPendingRequests)
        .withCacheSupplier(cacheSupplier)
        .build()
    ).map(kvToTuple)
      .mapValues(_.asScala.map(_.asScala))
  }

}

trait SCollectionSyntax {
  implicit def grpcSCollectionOps[T](sc: SCollection[T]): GrpcSCollectionOps[T] =
    new GrpcSCollectionOps(sc)
}
