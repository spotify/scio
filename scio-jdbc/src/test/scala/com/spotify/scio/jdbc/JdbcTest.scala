package com.spotify.scio.jdbc

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing.PipelineSpec

object JdbcJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.jdbcSelect[String](readOptions = null)
      .map(_ + "J")
      .saveAsJdbc(writeOptions = null)
    sc.close()
  }
}

// scalastyle:off no.whitespace.before.left.bracket
class JdbcTest extends PipelineSpec {

  def testJdbc(xs: String*): Unit = {
    JobTest[JdbcJob.type]
      .input(JdbcTestIO(TEST_READ_TABLE_NAME), Seq("a", "b", "c"))
      .output[String](JdbcTestIO(TEST_WRITE_TABLE_NAME))(_ should containInAnyOrder(xs))
      .run()
  }

  it should "pass correct JDBC" in {
    testJdbc("aJ", "bJ", "cJ")
  }

  it should "fail incorrect JDBC" in {
    an [AssertionError] should be thrownBy { testJdbc("aJ", "bJ")}
    an [AssertionError] should be thrownBy { testJdbc("aJ", "bJ", "cJ", "dJ")}
  }

}
// scalastyle:on no.whitespace.before.left.bracket