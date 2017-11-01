/*
 * Copyright 2017 Spotify AB.
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

import java.sql.ResultSet

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec

object JdbcJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.jdbcSelect(getReadOptions(opts))
      .map(_ + "J")
      .saveAsJdbc(getWriteOptions(opts))
    sc.close()
  }

  def getReadOptions(opts: CloudSqlOptions): JdbcReadOptions[String] =
    JdbcReadOptions(
      connectionOptions = getConnectionOptions(opts),
      query = "SELECT <this> FROM <this>",
      rowMapper = (rs: ResultSet) => rs.getString(1))

  def getWriteOptions(opts: CloudSqlOptions): JdbcWriteOptions[String] =
    JdbcWriteOptions[String](
      connectionOptions = getConnectionOptions(opts),
      statement = "INSERT INTO <this> VALUES( ?, ? ..?)")

  def connectionUrl(opts: CloudSqlOptions): String =
    s"jdbc:mysql://google/${opts.getCloudSqlDb}?" +
      s"cloudSqlInstance=${opts.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"

  def getConnectionOptions(opts: CloudSqlOptions): JdbcConnectionOptions =
    JdbcConnectionOptions(username = opts.getCloudSqlUsername,
      password = opts.getCloudSqlPassword,
      connectionUrl = connectionUrl(opts),
      classOf[java.sql.Driver])

}

class JdbcTest extends PipelineSpec {

  def testJdbc(xs: String*): Unit = {
    val args = Array(
      "--cloudSqlUsername=john",
      "--cloudSqlPassword=secret",
      "--cloudSqlDb=mydb",
      "--cloudSqlInstanceConnectionName=project-id:zone:db-instance-name")
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val readOpts = JdbcJob.getReadOptions(opts)
    val writeOpts = JdbcJob.getWriteOptions(opts)

    JobTest[JdbcJob.type]
      .args(args: _*)
      .input(JdbcIO(readOpts), Seq("a", "b", "c"))
      .output[String](JdbcIO(writeOpts))(_ should containInAnyOrder (xs))
      .run()
  }

  it should "pass correct JDBC" in {
    testJdbc("aJ", "bJ", "cJ")
  }

  it should "fail incorrect JDBC" in {
    // scalastyle:off no.whitespace.before.left.bracket
    an [AssertionError] should be thrownBy { testJdbc("aJ", "bJ") }
    an [AssertionError] should be thrownBy { testJdbc("aJ", "bJ", "cJ", "dJ") }
    // scalastyle:on no.whitespace.before.left.bracket
  }

}
