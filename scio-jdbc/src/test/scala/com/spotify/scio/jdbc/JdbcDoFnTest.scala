/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.jdbc

import com.spotify.scio.testing.PipelineSpec
import org.apache.commons.lang3.SerializationUtils

import java.io.PrintWriter
import java.sql.Connection
import java.util.logging.Logger
import javax.sql.DataSource

class JdbcDoFnTest extends PipelineSpec {

  "JdbcDoFn" should "return results from lookup func" in {
    val doFn = new JdbcDoFn[Int, String](_ => new DataSourceMock()) {
      override def lookup(connection: Connection, input: Int): String =
        input.toString
    }

    val output = runWithData(1 to 10)(_.parDo(doFn))
    output should contain theSameElementsAs (1 to 10).map(_.toString)
  }

  "JdbcDoFn" should "be serializable" in {
    val doFn = new JdbcDoFn[Int, String](_ => new DataSourceMock()) {
      override def lookup(connection: Connection, input: Int): String = ???
    }
    doFn.setup()
    doFn.startBundle()

    SerializationUtils.serialize(doFn)
  }
}

class DataSourceMock extends DataSource {
  override def getConnection: Connection = null

  override def getConnection(username: String, password: String): Connection = ???

  override def getLogWriter: PrintWriter = ???

  override def setLogWriter(out: PrintWriter): Unit = ???

  override def setLoginTimeout(seconds: Int): Unit = ???

  override def getLoginTimeout: Int = ???

  override def getParentLogger: Logger = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}
