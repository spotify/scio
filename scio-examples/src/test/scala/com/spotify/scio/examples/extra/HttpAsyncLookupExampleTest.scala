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

import java.io.IOException
import java.net.InetSocketAddress

import com.google.common.util.concurrent.MoreExecutors
import com.spotify.scio.io._
import com.spotify.scio.testing._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.BeforeAndAfterAll

class HttpAsyncLookupExampleTest extends PipelineSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = SimpleHttpServer.start

  override protected def afterAll(): Unit = SimpleHttpServer.stop

  "HttpAsyncLookupExample" should "work for a successful request" in {
    val expected = (1 to 5).map { i =>
      s"$i\t${i.toDouble}\ttest"
    }

    JobTest[com.spotify.scio.examples.extra.HttpAsyncLookupExample.type]
      .args("--output=out.text", "--url=http://localhost:9000/success")
      .output(TextIO("out.text"))(_ should containInAnyOrder(expected))
      .run()
  }

  it should "work with server error" in {
    val expected = (1 to 5).map { i =>
      s"$i\t${i.toDouble}\tDefault Value"
    }

    JobTest[com.spotify.scio.examples.extra.HttpAsyncLookupExample.type]
      .args("--output=out.text", "--url=http://localhost:9000/server-error")
      .output(TextIO("out.text"))(_ should containInAnyOrder(expected))
      .run()
  }

  it should "work with thrown exception" in {
    val expected = (1 to 5).map { i =>
      s"$i\t${i.toDouble}\tFailed"
    }

    JobTest[com.spotify.scio.examples.extra.HttpAsyncLookupExample.type]
      .args("--output=out.text", "--url=http://localhost:9000/exception")
      .output(TextIO("out.text"))(_ should containInAnyOrder(expected))
      .run()
  }
}

object SimpleHttpServer {
  lazy val server = HttpServer.create(new InetSocketAddress(9000), 0)

  def start(): Unit = {
    val requestHandler = new HttpRequestHandler
    server.createContext("/success", requestHandler)
    server.createContext("/server-error", requestHandler)
    server.createContext("/exception", requestHandler)
    server.setExecutor(MoreExecutors.directExecutor)
    server.start
  }

  def stop(): Unit = server.stop(0)
}

class HttpRequestHandler extends HttpHandler {

  override def handle(httpExchange: HttpExchange): Unit = {
    val path = httpExchange.getRequestURI.getPath
    path match {
      case "/success"      => write(httpExchange, "test", 200)
      case "/server-error" => write(httpExchange, "error", 503)
      case _               => throw new IOException("IOException")
    }
  }

  private def write(httpExchange: HttpExchange, message: String, code: Int): Unit = {
    httpExchange.getResponseHeaders.add("Content-Type", "text/plain; charset=UTF-8")
    val response = message.getBytes("UTF-8")
    httpExchange.sendResponseHeaders(code, response.length)
    val os = httpExchange.getResponseBody
    os.write(response)
    os.close
  }
}
