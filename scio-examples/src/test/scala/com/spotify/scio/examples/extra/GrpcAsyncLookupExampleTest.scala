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

package com.spotify.scio.examples.extra

import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio.io.TextIO
import com.spotify.scio.proto.SimpleGrpc.{TrackMetadataReply, TrackMetadataRequest}
import com.spotify.scio.proto.TrackServiceGrpc
import com.spotify.scio.testing.PipelineSpec
import io.grpc.stub.StreamObserver
import io.grpc.{Server, ServerBuilder}
import org.scalatest.BeforeAndAfterAll

class GrpcAsyncLookupExampleTest extends PipelineSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = TestTrackService.start()

  override protected def afterAll(): Unit = TestTrackService.stop()

  "GrpcAsyncLookupExample" should "work for a successful and failed requests" in {
    val expected = (1 to 5).map { i =>
      if (i == 5) {
        s"userId$i\tFailed"
      } else {
        s"userId$i\tuserId$i-success"
      }
    }

    JobTest[com.spotify.scio.examples.extra.GrpcAsyncLookupExample.type]
      .args("--output=out.text", "--host=localhost", s"--port=${TestTrackService.Port}")
      .output(TextIO("out.text"))(_ should containInAnyOrder(expected))
      .run()
    ()
  }
}

class TestTrackService {
  self =>
  private[this] var GrpcServer: Server = _

  def start(): Unit = {
    GrpcServer = ServerBuilder
      .forPort(TestTrackService.Port)
      .addService(new TrackServiceImpl)
      .build
      .start
    sys.addShutdownHook {
      self.stop()
      ()
    }
    ()
  }

  def stop(): Unit = {
    if (GrpcServer != null) {
      GrpcServer.shutdown()
      ()
    }
  }
}

private class TrackServiceImpl extends TrackServiceGrpc.TrackServiceImplBase {

  override def requestTrackName(
    request: TrackMetadataRequest,
    responseObserver: StreamObserver[TrackMetadataReply]
  ): Unit = {
    if (request.getId.endsWith("5")) {
      responseObserver.onError(new RuntimeException("failed"))
    } else {
      val reply = TrackMetadataReply.newBuilder().setName(request.getId + "-success").build()
      responseObserver.onNext(reply)
      responseObserver.onCompleted
    }
  }
}

object TestTrackService {

  val Server = new TestTrackService

  def start(): Unit = Server.start()

  def stop(): Unit = Server.stop()

  val Port = ThreadLocalRandom.current().nextInt(60000, 65000)
}
