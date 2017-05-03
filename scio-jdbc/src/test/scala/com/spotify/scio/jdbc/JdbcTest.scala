/*
 * Copyright 2016 Spotify AB.
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
