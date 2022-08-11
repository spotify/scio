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
import com.spotify.scio.transforms.JavaAsyncConverters._
import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions.kvToTuple
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import io.grpc.Channel
import io.grpc.stub.{AbstractFutureStub, AbstractStub, StreamObserver}
import org.apache.beam.sdk.transforms.ParDo

import scala.util.Try
import scala.jdk.CollectionConverters._

class MessageSCollectionOps[Request](private val self: SCollection[Request]) extends AnyVal {

  def grpcLookup[Response: Coder, Client <: AbstractFutureStub[Client]](
    channelSupplier: () => Channel,
    clientFactory: Channel => Client,
    maxPendingRequests: Int
  )(f: (Client, Request) => ListenableFuture[Response]): SCollection[(Request, Try[Response])] = {
    implicit val requestCoder: Coder[Request] = self.coder
    self
      .parDo(
        GrpcDoFn
          .newBuilder[Request, Response, Client]()
          .withChannelSupplier(() => ClosureCleaner.clean(channelSupplier)())
          .withNewClientFn(Functions.serializableFn(clientFactory))
          .withLookupFn(Functions.serializableBiFn(f))
          .withMaxPendingRequests(maxPendingRequests)
          .build()
      )
      .map(kvToTuple)
      .mapValues(_.asScala)
  }

  def grpcLookupStream[Response: Coder, Client <: AbstractStub[Client]](
    channelSupplier: () => Channel,
    clientFactory: Channel => Client,
    maxPendingRequests: Int
  )(
    f: (Client, StreamObserver[Response], Request) => Unit
  ): SCollection[(Request, Try[Iterable[Response]])] = {
    implicit val requestCoder: Coder[Request] = self.coder
    val lookupFn = (client: Client, request: Request) => {
      val observer = new StreamObservableFuture[Response]()
      f(client, observer, request)
      observer
    }
    self
      .parDo(
        GrpcDoFn
          .newBuilder[Request, java.lang.Iterable[Response], Client]()
          .withChannelSupplier(() => ClosureCleaner.clean(channelSupplier)())
          .withNewClientFn(Functions.serializableFn(clientFactory))
          .withLookupFn(Functions.serializableBiFn(lookupFn))
          .withMaxPendingRequests(maxPendingRequests)
          .build()
      )
      .map(kvToTuple)
      .mapValues(_.asScala.map(_.asScala))
  }

}

trait SCollectionSyntax {
  implicit def messageSCollectionOps[T](sc: SCollection[T]): MessageSCollectionOps[T] =
    new MessageSCollectionOps(sc)
}
