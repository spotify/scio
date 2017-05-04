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
    val (_opt, _args) = ScioContext.parseArguments[CloudSQLOptions](cmdlineArgs)
    val sc = ScioContext(_opt)
    sc.jdbcSelect[String](readOptions = getReadOptions(_opt))
      .map(_ + "J")
      .saveAsJdbc(writeOptions = getWriteOptions(_opt))
    sc.close()
  }

  def getReadOptions(cloudOpt: CloudSQLOptions): JdbcReadOptions[String] = {
    JdbcReadOptions(dbConnectionOptions = getDbConnectionOpt(cloudOpt),
      query = "SELECT <this> FROM <this>",
      rowMapper = (rs: ResultSet) => rs.getString(1))
  }

  def getWriteOptions(cloudOpt: CloudSQLOptions): JdbcWriteOptions[String] = {
    JdbcWriteOptions[String](dbConnectionOptions = getDbConnectionOpt(cloudOpt),
      statement = "INSERT INTO <this> VALUES( ?, ? ..?)")
  }

  def connectionUrl(cloudOption: CloudSQLOptions): String = {
    s"jdbc:mysql://google/${cloudOption.getCloudSqlDb}?" +
      s"cloudSqlInstance=${cloudOption.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"
  }

  def getDbConnectionOpt(cloudOption: CloudSQLOptions): DbConnectionOptions = {
    DbConnectionOptions(username = cloudOption.getCloudSqlUser,
      password = cloudOption.getCloudSqlPassword,
      connectionUrl = connectionUrl(cloudOption),
      classOf[java.sql.Driver])
  }
}

// scalastyle:off no.whitespace.before.left.bracket
class JdbcTest extends PipelineSpec {

  def testJdbc(xs: String*): Unit = {
    JobTest[JdbcJob.type]
      .args("--cloudSqlUser=john", "--cloudSqlPassword=secret", "--cloudSqlDb=mydb",
        "--cloudSqlInstanceConnectionName=my-project-Id:zonex:db-instance-name")
      .input(JdbcIO("johnsecret"), Seq("a", "b", "c"))
      .output[String](JdbcIO("johnsecret"))(_ should containInAnyOrder (xs))
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
