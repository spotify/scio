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

import com.google.common.cache.{Cache, CacheBuilder}
import com.spotify.concat.v1.ConcatServiceGrpc.{
  ConcatServiceFutureStub,
  ConcatServiceImplBase,
  ConcatServiceStub
}
import com.spotify.concat.v1._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{Server, ServerBuilder}
import org.scalatest.BeforeAndAfterAll

import java.net.ServerSocket
import scala.util.{Success, Try}

object GrpcDoFnTest {

  // Find port in the companion object
  // So we can serialize the target uri
  // without having to reference the GrpcDoFnTest class
  val LocalPort: Int = {
    val socket = new ServerSocket(0) // when port == 0, binds to any available port
    val port = socket.getLocalPort
    socket.close()
    port
  }

  val ServiceUri: String = s"dns:///localhost:$LocalPort"

  def concatOrdered(request: ConcatRequest): ConcatResponse = ConcatResponse
    .newBuilder()
    .setResponse(request.getStringOne + request.getStringTwo)
    .build()

  def concatReversed(request: ConcatRequest): ConcatResponse = ConcatResponse
    .newBuilder()
    .setResponse(request.getStringTwo + request.getStringOne)
    .build()

  class ConcatServiceImpl extends ConcatServiceImplBase {
    override def concat(
      request: ConcatRequest,
      responseObserver: StreamObserver[ConcatResponse]
    ): Unit = {
      responseObserver.onNext(concatOrdered(request))
      responseObserver.onCompleted()
    }

    // Returns the concatenations of both words in either order
    override def concatServerStreaming(
      request: ConcatRequest,
      responseObserver: StreamObserver[ConcatResponse]
    ): Unit = {
      responseObserver.onNext(concatOrdered(request))
      responseObserver.onNext(concatReversed(request))
      responseObserver.onCompleted()
    }
  }

}

class GrpcDoFnTest extends PipelineSpec with BeforeAndAfterAll {

  import GrpcDoFnTest._

  val server: Server = ServerBuilder
    .forPort(LocalPort)
    .addService(new ConcatServiceImpl())
    .asInstanceOf[ServerBuilder[_]]
    .build()

  override def beforeAll(): Unit = server.start()

  override def afterAll(): Unit = server.shutdown()

  "GrpcDoFn" should "issue request and propagate response" in {
    val input = (0 to 3).map { i =>
      ConcatRequest
        .newBuilder()
        .setStringOne(i.toString)
        .setStringTwo(i.toString)
        .build()
    }
    val expected: Seq[(ConcatRequest, Try[ConcatResponse])] = input.map { req =>
      val resp = concatOrdered(req)
      req -> Success(resp)
    }

    runWithContext { sc =>
      val result = sc
        .parallelize(input)
        .grpcLookup[ConcatResponse, ConcatServiceFutureStub](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          2
        )(_.concat)

      result should containInAnyOrder(expected)
    }
  }

  it should "issue request and propagate streamed responses" in {
    val input = (0 to 3).map { i =>
      ConcatRequest
        .newBuilder()
        .setStringOne(i.toString)
        .setStringTwo((Char.char2int('a') + i).toChar.toString)
        .build()
    }
    val expected: Seq[(ConcatRequest, Try[Iterable[ConcatResponse]])] = input.map { req =>
      val resps = Seq(concatOrdered(req), concatReversed(req))
      req -> Success(resps)
    }

    runWithContext { sc =>
      val result = sc
        .parallelize(input)
        .grpcLookupStream[ConcatResponse, ConcatServiceStub](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newStub,
          2
        )(_.concatServerStreaming)

      result should containInAnyOrder(expected)
    }
  }

  it should "return cached responses" in {
    val request = ConcatRequest
      .newBuilder()
      .setStringOne("one")
      .setStringTwo("two")
      .build()

    // not a real concat
    val response = ConcatResponse
      .newBuilder()
      .setResponse("twelve")
      .build()

    // create initialized cache
    val cacheSupplier = new CacheSupplier[ConcatRequest, ConcatResponse] {
      override def get(): Cache[ConcatRequest, ConcatResponse] = {
        val cache = CacheBuilder.newBuilder().build[ConcatRequest, ConcatResponse]()
        cache.put(request, response)
        cache
      }
    }

    val input = Seq(request)
    val expected: Seq[(ConcatRequest, Try[ConcatResponse])] = Seq(
      request -> Success(response)
    )

    runWithContext { sc =>
      val result = sc
        .parallelize(input)
        .grpcLookup[ConcatResponse, ConcatServiceFutureStub](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          2,
          cacheSupplier
        )(_.concat)

      result should containInAnyOrder(expected)
    }
  }

}
