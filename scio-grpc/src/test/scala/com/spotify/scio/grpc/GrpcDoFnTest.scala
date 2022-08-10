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

import com.spotify.concat.v1.ConcatServiceGrpc.{
  ConcatServiceFutureStub,
  ConcatServiceImplBase,
  ConcatServiceStub
}
import com.spotify.concat.v1._
import com.spotify.scio.testing.PipelineSpec
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

  val ServiceUri = s"dns:///localhost:$LocalPort"

  class ConcatServiceImpl extends ConcatServiceImplBase {
    override def concat(
      request: ConcatRequest,
      responseObserver: StreamObserver[ConcatResponse]
    ): Unit = {
      responseObserver.onNext(
        ConcatResponse
          .newBuilder()
          .setResponse(request.getStringOne + request.getStringTwo)
          .build()
      )

      responseObserver.onCompleted()
    }

    // Returns the concatenations of both words in either order
    override def concatServerStreaming(
      request: ConcatRequest,
      responseObserver: StreamObserver[ConcatResponse]
    ): Unit = {
      responseObserver.onNext(
        ConcatResponse
          .newBuilder()
          .setResponse(request.getStringOne + request.getStringTwo)
          .build()
      )

      responseObserver.onNext(
        ConcatResponse
          .newBuilder()
          .setResponse(request.getStringTwo + request.getStringOne)
          .build()
      )

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
    runWithContext { sc =>
      val result = sc
        .parallelize(0 to 3)
        .grpcLookup[ConcatResponse, ConcatServiceFutureStub](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          2
        ) { case (client, i) =>
          val request = ConcatRequest
            .newBuilder()
            .setStringOne(i.toString)
            .setStringTwo(i.toString)
            .build()
          client.concat(request)
        }

      result should containInAnyOrder(
        Seq[(Int, Try[ConcatResponse])](
          0 -> Success(ConcatResponse.newBuilder().setResponse("00").build()),
          1 -> Success(ConcatResponse.newBuilder().setResponse("11").build()),
          2 -> Success(ConcatResponse.newBuilder().setResponse("22").build()),
          3 -> Success(ConcatResponse.newBuilder().setResponse("33").build())
        )
      )
    }
  }

  it should "issue request and propagate streamed responses" in {
    runWithContext { sc =>
      val result = sc
        .parallelize(0 to 3)
        .grpcLookupStream[ConcatResponse, ConcatServiceStub](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newStub,
          2
        ) { case (client, observer, i) =>
          val request = ConcatRequest
            .newBuilder()
            .setStringOne(i.toString)
            .setStringTwo((Char.char2int('a') + i).toChar.toString)
            .build()
          client.concatServerStreaming(request, observer)
        }

      result should containInAnyOrder(
        Seq[(Int, Try[Iterable[ConcatResponse]])](
          0 -> Success(
            List(
              ConcatResponse.newBuilder().setResponse("0a").build(),
              ConcatResponse.newBuilder().setResponse("a0").build()
            )
          ),
          1 -> Success(
            List(
              ConcatResponse.newBuilder().setResponse("1b").build(),
              ConcatResponse.newBuilder().setResponse("b1").build()
            )
          ),
          2 -> Success(
            List(
              ConcatResponse.newBuilder().setResponse("2c").build(),
              ConcatResponse.newBuilder().setResponse("c2").build()
            )
          ),
          3 -> Success(
            List(
              ConcatResponse.newBuilder().setResponse("3d").build(),
              ConcatResponse.newBuilder().setResponse("d3").build()
            )
          )
        )
      )
    }

  }

}
