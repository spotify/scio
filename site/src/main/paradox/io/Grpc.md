# GRPC

Scio supports lookups via [GRPC](https://grpc.io/) in the `scio-grpc` artifact.

Given an `SCollection` of GRPC request objects (`ConcatRequest` below), @scaladoc[grpcLookup](com.spotify.scio.grpc.GrpcSCollectionOps#grpcLookup[Response,Client%3C:io.grpc.stub.AbstractFutureStub[Client]](channelSupplier:()=%3Eio.grpc.Channel,clientFactory:io.grpc.Channel=%3EClient,maxPendingRequests:Int,cacheSupplier:com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier[Request,Response])(f:Client=%3E(Request=%3Ecom.google.common.util.concurrent.ListenableFuture[Response]))(implicitevidence$1:com.spotify.scio.coders.Coder[Response]):com.spotify.scio.values.SCollection[(Request,scala.util.Try[Response])]) (or @scaladoc[grpcLookupStream](com.spotify.scio.grpc.GrpcSCollectionOps#grpcLookupStream[Response,Client%3C:io.grpc.stub.AbstractStub[Client]](channelSupplier:()=%3Eio.grpc.Channel,clientFactory:io.grpc.Channel=%3EClient,maxPendingRequests:Int,cacheSupplier:com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier[Request,Iterable[Response]])(f:Client=%3E((Request,io.grpc.stub.StreamObserver[Response])=%3EUnit))(implicitevidence$2:com.spotify.scio.coders.Coder[Response]):com.spotify.scio.values.SCollection[(Request,scala.util.Try[Iterable[Response]])]) for iterable responses,
@scaladoc[grpcBatchLookup](com.spotify.scio.grpc.GrpcSCollectionOps#grpcLookupStream[Response,Client%3C:io.grpc.stub.AbstractStub[Client]](channelSupplier:()=%3Eio.grpc.Channel,clientFactory:io.grpc.Channel=%3EClient,maxPendingRequests:Int,cacheSupplier:com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier[Request,Iterable[Response]])(f:Client=%3E((Request,io.grpc.stub.StreamObserver[Response])=%3EUnit))(implicitevidence$2:com.spotify.scio.coders.Coder[Response]):com.spotify.scio.values.SCollection[(Request,scala.util.Try[Iterable[Response]])]) for batched calls) provides a concise syntax for handling responses:

```scala
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.grpc._
import com.spotify.concat.v1._
import io.grpc.netty.NettyChannelBuilder

val ServiceUri: String = "dns://localhost:50051"
val maxPendingRequests = 10
val requests: SCollection[ConcatRequest] = ???
requests
  .grpcLookup[ConcatResponse, ConcatServiceGrpc.ConcatServiceFutureStub](
    () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
    ConcatServiceGrpc.newFutureStub,
    maxPendingRequests
  )(_.concat)
```
