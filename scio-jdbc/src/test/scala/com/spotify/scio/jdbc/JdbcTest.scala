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

package com.spotify.scio.jdbc

import java.sql.ResultSet

import com.spotify.scio._
import org.apache.beam.sdk.io.{jdbc => beam}
import com.spotify.scio.testing._

object JdbcJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.jdbcSelect(getReadOptions(opts))
      .map(_ + "J")
      .saveAsJdbc(getWriteOptions(opts))
    sc.run()
    ()
  }

  def getReadOptions(opts: CloudSqlOptions): JdbcReadOptions[String] =
    JdbcReadOptions(
      connectionOptions = getConnectionOptions(opts),
      query = "SELECT <this> FROM <this>",
      rowMapper = (rs: ResultSet) => rs.getString(1)
    )

  def getWriteOptions(opts: CloudSqlOptions): JdbcWriteOptions[String] =
    JdbcWriteOptions[String](
      connectionOptions = getConnectionOptions(opts),
      statement = "INSERT INTO <this> VALUES( ?, ? ..?)"
    )

  def connectionUrl(opts: CloudSqlOptions): String =
    s"jdbc:mysql://google/${opts.getCloudSqlDb}?" +
      s"cloudSqlInstance=${opts.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"

  def getConnectionOptions(opts: CloudSqlOptions): JdbcConnectionOptions =
    JdbcConnectionOptions(
      username = opts.getCloudSqlUsername,
      password = Some(opts.getCloudSqlPassword),
      connectionUrl = connectionUrl(opts),
      classOf[java.sql.Driver]
    )
}

class JdbcTest extends PipelineSpec {
  def testJdbc(xs: String*): Unit = {
    val args = Seq(
      "--cloudSqlUsername=john",
      "--cloudSqlPassword=secret",
      "--cloudSqlDb=mydb",
      "--cloudSqlInstanceConnectionName=project-id:zone:db-instance-name"
    )
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args.toArray)
    val readOpts = JdbcJob.getReadOptions(opts)
    val writeOpts = JdbcJob.getWriteOptions(opts)

    JobTest[JdbcJob.type]
      .args(args: _*)
      .input(JdbcIO[String](readOpts), Seq("a", "b", "c"))
      .output(JdbcIO[String](writeOpts))(coll => coll should containInAnyOrder(xs))
      .run()
  }

  it should "pass correct JDBC" in {
    testJdbc("aJ", "bJ", "cJ")
  }

  it should "fail incorrect JDBC" in {
    an[AssertionError] should be thrownBy { testJdbc("aJ", "bJ") }
    an[AssertionError] should be thrownBy { testJdbc("aJ", "bJ", "cJ", "dJ") }
  }

  it should "connnect via JDBC without a password" in {
    val args = Seq(
        "--cloudSqlUsername=john",
        "--cloudSqlDb=mydb",
        "--cloudSqlInstanceConnectionName=project-id:zone:db-instance-name"
      )
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args.toArray)
    val readOpts = JdbcJob.getReadOptions(opts)
    val writeOpts = JdbcJob.getWriteOptions(opts)

    val expected = Seq("aJ", "bJ", "cJ")

    JobTest[JdbcJob.type]
      .args(args: _*)
      .input(JdbcIO[String](readOpts), Seq("a", "b", "c"))
      .output(JdbcIO[String](writeOpts))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  it should "generate connection string with password" in {
    val password = JdbcConnectionOptions(
      username = "user",
      password = Some("pass"),
      connectionUrl = "foo",
      driverClass = classOf[java.sql.Driver]
    )
    JdbcIO.jdbcIoId(password, "query") shouldEqual "user:pass@foo:query"
  }

  it should "generate connection string without password" in {
    val noPassword = JdbcConnectionOptions(
      username = "user",
      password = None,
      connectionUrl = "foo",
      driverClass = classOf[java.sql.Driver]
    )
    JdbcIO.jdbcIoId(noPassword, "query") shouldEqual "user@foo:query"
  }

  it should "generate datasource config with password" in {
    val opts = JdbcConnectionOptions(
      username = "user",
      password = Some("pass"),
      connectionUrl = "foo",
      driverClass = classOf[java.sql.Driver]
    )
    val expected = beam.JdbcIO.DataSourceConfiguration
      .create(classOf[java.sql.Driver].getCanonicalName, "foo")
      .withUsername("user")
      .withPassword("pass")

    JdbcIO.dataSourceConfiguration(opts).toString shouldBe expected.toString
  }

  it should "generate datasource config without password" in {
    val opts = JdbcConnectionOptions(
      username = "user",
      password = None,
      connectionUrl = "foo",
      driverClass = classOf[java.sql.Driver]
    )
    val expected = beam.JdbcIO.DataSourceConfiguration
      .create(classOf[java.sql.Driver].getCanonicalName, "foo")
      .withUsername("user")

    JdbcIO.dataSourceConfiguration(opts).toString shouldBe expected.toString
  }
}
