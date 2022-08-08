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
      .applyTransform(
        ParDo.of(
          GrpcDoFn
            .newBuilder[Request, Response, Client]()
            .withChannelSupplier(() => ClosureCleaner.clean(channelSupplier)())
            .withNewClientFn(Functions.serializableFn(clientFactory))
            .withLookupFn(Functions.serializableBiFn(f))
            .withMaxPendingRequests(maxPendingRequests)
            .build()
        )
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
      .applyTransform(
        ParDo.of(
          GrpcDoFn
            .newBuilder[Request, java.lang.Iterable[Response], Client]()
            .withChannelSupplier(() => ClosureCleaner.clean(channelSupplier)())
            .withNewClientFn(Functions.serializableFn(clientFactory))
            .withLookupFn(Functions.serializableBiFn(lookupFn))
            .withMaxPendingRequests(maxPendingRequests)
            .build()
        )
      )
      .map(kvToTuple)
      .mapValues(_.asScala.map(_.asScala))
  }

}

trait SCollectionSyntax {
  implicit def messageSCollectionOps[T](sc: SCollection[T]): MessageSCollectionOps[T] =
    new MessageSCollectionOps(sc)
}
