package com.spotify.scio.jdbc

import com.spotify.scio.testing.PipelineSpec

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
