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
import org.apache.commons.lang3.tuple.Pair

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

  def grpcBatchLookup[
    BatchRequest,
    BatchResponse,
    Response: Coder,
    Client <: AbstractFutureStub[Client]
  ](
    channelSupplier: () => Channel,
    clientFactory: Channel => Client,
    batchSize: Int,
    batchRequestFn: Seq[Request] => BatchRequest,
    batchResponseFn: BatchResponse => Seq[(String, Response)],
    idExtractorFn: Request => String,
    maxPendingRequests: Int,
    cacheSupplier: CacheSupplier[String, Response] = new NoOpCacheSupplier[String, Response]()
  )(
    f: Client => BatchRequest => ListenableFuture[BatchResponse]
  ): SCollection[(Request, Try[Response])] = self.transform { in =>
    import self.coder
    val cleanedChannelSupplier = ClosureCleaner.clean(channelSupplier)
    val serializableClientFactory = Functions.serializableFn(clientFactory)
    val serializableLookupFn =
      Functions.serializableBiFn[Client, BatchRequest, ListenableFuture[BatchResponse]] {
        (client, request) => f(client)(request)
      }

    val serializableBatchRequestFn =
      Functions.serializableFn[java.util.List[Request], BatchRequest] { inputs =>
        batchRequestFn(inputs.asScala.toSeq)
      }

    val serializableBatchResponseFn =
      Functions.serializableFn[BatchResponse, java.util.List[Pair[String, Response]]] {
        batchResponse =>
          batchResponseFn(batchResponse).map { case (input, output) =>
            Pair.of(input, output)
          }.asJava
      }
    val serializableIdExtractorFn = Functions.serializableFn(idExtractorFn)

    in.parDo(
      GrpcBatchDoFn
        .newBuilder[Request, BatchRequest, BatchResponse, Response, Client]()
        .withChannelSupplier(() => cleanedChannelSupplier())
        .withNewClientFn(serializableClientFactory)
        .withLookupFn(serializableLookupFn)
        .withMaxPendingRequests(maxPendingRequests)
        .withBatchSize(batchSize)
        .withBatchRequestFn(serializableBatchRequestFn)
        .withBatchResponseFn(serializableBatchResponseFn)
        .withIdExtractorFn(serializableIdExtractorFn)
        .withCacheSupplier(cacheSupplier)
        .build()
    ).map(kvToTuple _)
      .mapValues(_.asScala)
  }
}

trait SCollectionSyntax {
  implicit def grpcSCollectionOps[T](sc: SCollection[T]): GrpcSCollectionOps[T] =
    new GrpcSCollectionOps(sc)
}
