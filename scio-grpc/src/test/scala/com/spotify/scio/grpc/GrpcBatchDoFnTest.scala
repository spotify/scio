/*
 * Copyright 2023 Spotify AB
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
import com.spotify.concat.v1.ConcatServiceGrpc.{ConcatServiceFutureStub, ConcatServiceImplBase}
import com.spotify.concat.v1._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{Server, ServerBuilder}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.scalatest.BeforeAndAfterAll

import java.net.ServerSocket
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object GrpcBatchDoFnTest {

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

  def processBatch(request: BatchRequest): BatchResponse =
    BatchResponse
      .newBuilder()
      .addAllResponse(
        request.getRequestsList
          .stream()
          .map[ConcatResponseWithID](concat)
          .collect(Collectors.toList())
      )
      .build()

  def concatBatchRequest(inputs: Seq[ConcatRequestWithID]): BatchRequest =
    BatchRequest
      .newBuilder()
      .addAllRequests(inputs.asJava)
      .build()

  def concatBatchResponse(response: BatchResponse): Seq[(String, ConcatResponseWithID)] =
    response.getResponseList.asScala.toSeq.map(e => (e.getRequestId, e))

  def idExtractor(concatRequest: ConcatRequestWithID): String =
    concatRequest.getRequestId

  def concat(request: ConcatRequestWithID): ConcatResponseWithID = ConcatResponseWithID
    .newBuilder()
    .setRequestId(request.getRequestId)
    .setResponse(request.getStringOne + request.getStringTwo)
    .build()

  class BatchedConcatServiceImpl extends ConcatServiceImplBase {
    override def batchConcat(
      request: BatchRequest,
      responseObserver: StreamObserver[BatchResponse]
    ): Unit = {
      responseObserver.onNext(processBatch(request))
      responseObserver.onCompleted()
    }
  }
}

class GrpcBatchDoFnTest extends PipelineSpec with BeforeAndAfterAll {

  import GrpcBatchDoFnTest._

  val server: Server = ServerBuilder
    .forPort(LocalPort)
    .addService(new BatchedConcatServiceImpl())
    .asInstanceOf[ServerBuilder[_]]
    .build()

  override def beforeAll(): Unit = server.start()

  override def afterAll(): Unit = server.shutdown()

  "GrpcBatchDoFn" should "issue request and propagate response" in {

    val input = (0 to 10).map { i =>
      ConcatRequestWithID
        .newBuilder()
        .setRequestId(i.toString)
        .setStringOne(i.toString)
        .setStringTwo(i.toString)
        .build()
    }

    val expected: Seq[(ConcatRequestWithID, Try[ConcatResponseWithID])] = input.map { req =>
      val resp = concat(req)
      req -> Success(resp)
    }

    runWithContext { sc =>
      val result = sc
        .parallelize(input)
        .grpcBatchLookup[
          BatchRequest,
          BatchResponse,
          ConcatResponseWithID,
          ConcatServiceFutureStub
        ](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          2,
          concatBatchRequest,
          concatBatchResponse,
          idExtractor,
          2
        )(_.batchConcat)

      result should containInAnyOrder(expected)
    }
  }

  it should "return cached responses" in {
    val request = ConcatRequestWithID
      .newBuilder()
      .setRequestId("1")
      .setStringOne("one")
      .setStringTwo("two")
      .build()

    // not a real concat
    val response = ConcatResponseWithID
      .newBuilder()
      .setRequestId("1")
      .setResponse("twelve")
      .build()

    // create initialized cache
    val cacheSupplier = new CacheSupplier[String, ConcatResponseWithID] {
      override def get(): Cache[String, ConcatResponseWithID] = {
        val cache = CacheBuilder.newBuilder().build[String, ConcatResponseWithID]()
        cache.put(request.getRequestId, response)
        cache
      }
    }

    val input = Seq(request)
    val expected: Seq[(ConcatRequestWithID, Try[ConcatResponseWithID])] = Seq(
      request -> Success(response)
    )

    runWithContext { sc =>
      val result = sc
        .parallelize(input)
        .grpcBatchLookup[
          BatchRequest,
          BatchResponse,
          ConcatResponseWithID,
          ConcatServiceFutureStub
        ](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          2,
          concatBatchRequest,
          concatBatchResponse,
          idExtractor,
          2,
          cacheSupplier
        )(_.batchConcat)

      result should containInAnyOrder(expected)
    }
  }

  it should "propagate results if elements have the same id" in {
    val input = for {
      _ <- 0 to 5
      i <- 0 to 10
    } yield ConcatRequestWithID
      .newBuilder()
      .setRequestId(i.toString)
      .setStringOne(i.toString)
      .setStringTwo(i.toString)
      .build()

    val expected: Seq[(ConcatRequestWithID, Try[ConcatResponseWithID])] = input.map { req =>
      val resp = concat(req)
      req -> Success(resp)
    }

    runWithContext { sc =>
      // use flatMap to make sure all elements are in the same bundle
      val result = sc
        .parallelize(Seq(()))
        .flatMap(_ => input)
        .grpcBatchLookup[
          BatchRequest,
          BatchResponse,
          ConcatResponseWithID,
          ConcatServiceFutureStub
        ](
          () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
          ConcatServiceGrpc.newFutureStub,
          50,
          concatBatchRequest,
          concatBatchResponse,
          idExtractor,
          2
        )(_.batchConcat)

      result should containInAnyOrder(expected)
    }
  }

  it should "throw an IllegalStateException if gRPC response contains unknown ids" in {
    val input = (0 to 1).map { i =>
      ConcatRequestWithID
        .newBuilder()
        .setRequestId(i.toString)
        .setStringOne(i.toString)
        .setStringTwo(i.toString)
        .build()
    }

    assertThrows[IllegalStateException] {
      try {
        runWithContext { sc =>
          sc.parallelize(input)
            .grpcBatchLookup[
              BatchRequest,
              BatchResponse,
              ConcatResponseWithID,
              ConcatServiceFutureStub
            ](
              () => NettyChannelBuilder.forTarget(ServiceUri).usePlaintext().build(),
              ConcatServiceGrpc.newFutureStub,
              2,
              concatBatchRequest,
              r => r.getResponseList.asScala.toSeq.map(e => ("WrongID-" + e.getRequestId, e)),
              idExtractor,
              2
            )(_.batchConcat)
        }
      } catch {
        case e: PipelineExecutionException =>
          e.getMessage should include("Expected requestCount == responseCount")
          throw e.getCause
      }
    }
  }
}
