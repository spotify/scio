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
import com.spotify.scio.proto.simple_grpc.{
  CustomerOrderReply,
  CustomerOrderRequest,
  CustomerOrderServiceGrpc
}
import com.spotify.scio.testing.PipelineSpec
import io.grpc.{Server, ServerBuilder}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

class GrpcAsyncLookupExampleTest extends PipelineSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = TestCustomerOrderServer.start()

  override protected def afterAll(): Unit = TestCustomerOrderServer.stop()

  "GrpcAsyncLookupExample" should "work for a successful and failed requests" in {
    val expected = (1 to 5).map { i =>
      if (i == 5) {
        s"userId$i\tFailed"
      } else {
        s"userId$i\tuserId$i-success"
      }
    }

    JobTest[com.spotify.scio.examples.extra.GrpcAsyncLookupExample.type]
      .args("--output=out.text", "--host=localhost", s"--port=${TestCustomerOrderServer.Port}")
      .output(TextIO("out.text"))(_ should containInAnyOrder(expected))
      .run()
    ()
  }
}

class TestCustomerOrderServer(executionContext: ExecutionContext) {
  self =>
  private[this] var GrpcServer: Server = _

  def start(): Unit = {
    GrpcServer = ServerBuilder
      .forPort(TestCustomerOrderServer.Port)
      .addService(
        CustomerOrderServiceGrpc
          .bindService(new CustomerOrderServiceImpl, executionContext)
      )
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

private class CustomerOrderServiceImpl extends CustomerOrderServiceGrpc.CustomerOrderService {

  override def requestOrder(request: CustomerOrderRequest): Future[CustomerOrderReply] = {
    if (request.id.endsWith("5")) {
      Future.failed(new RuntimeException("failed"))
    } else {
      Future {
        CustomerOrderReply.of(request.id + "-success")
      }
    }
  }
}

object TestCustomerOrderServer {

  val Server = new TestCustomerOrderServer(ExecutionContext.global)

  def start(): Unit = Server.start()

  def stop(): Unit = Server.stop()

  val Port = ThreadLocalRandom.current().nextInt(60000, 65000)
}
