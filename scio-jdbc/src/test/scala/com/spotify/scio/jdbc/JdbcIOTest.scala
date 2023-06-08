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

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.{EqualNamePTransformMatcher, TransformFinder}
import org.apache.beam.sdk.io.jdbc.{JdbcIO => BJdbcIO}
import org.apache.beam.sdk.transforms.display.DisplayData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import scala.jdk.CollectionConverters._

case class Foo(field: String)

object JdbcIOTest {
  private val ReadQueryId = DisplayData.Identifier.of(
    DisplayData.Path.root(),
    classOf[BJdbcIO.Read[_]],
    "query"
  )

  private val WriteStatementId = DisplayData.Identifier.of(
    DisplayData.Path.absolute("fn"),
    Class.forName("org.apache.beam.sdk.io.jdbc.JdbcIO$WriteFn"),
    "statement"
  )
}

class JdbcIOTest extends AnyFlatSpec with Matchers {
  import JdbcIOTest._

  it must "add to pipeline overridden Read transform" in {
    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val name = "jdbcSelect"
    val sc = ScioContext(opts)
    sc.withName(name)
      .jdbcSelect[String](
        getConnectionOptions(opts),
        "initial query",
        configOverride = (x: BJdbcIO.Read[String]) => x.withQuery("overridden query")
      )(rs => rs.getString(1))

    val finder = new TransformFinder(new EqualNamePTransformMatcher(name))
    sc.pipeline.traverseTopologically(finder)
    val transform = finder.result().head
    val displayData = DisplayData.from(transform).asMap().asScala

    displayData should contain key ReadQueryId
    displayData(ReadQueryId).getValue should be("overridden query")
  }

  it must "add to pipeline overridden Write transform" in {
    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val sc = ScioContext(opts)
    val name = "saveAsJdbc"
    sc.parallelize(List("1", "2", "3"))
      .withName(name)
      .saveAsJdbc(
        getConnectionOptions(opts),
        "INSERT INTO <this> VALUES(?)",
        configOverride = _.withStatement("updated statement")
      )((x, ps) => ps.setString(0, x))

    // find the underlying jdbc write
    val finder = new TransformFinder(new EqualNamePTransformMatcher(name + "/ParDo(Write)"))
    sc.pipeline.traverseTopologically(finder)
    val transform = finder.result().head
    val displayData = DisplayData.from(transform).asMap().asScala
    println(displayData)
    displayData should contain key WriteStatementId
    displayData(WriteStatementId).getValue should be("updated statement")
  }

  def getConnectionOptions(opts: CloudSqlOptions): JdbcConnectionOptions =
    JdbcConnectionOptions(
      username = opts.getCloudSqlUsername,
      password = Some(opts.getCloudSqlPassword),
      connectionUrl = connectionUrl(opts),
      classOf[java.sql.Driver]
    )

  def connectionUrl(opts: CloudSqlOptions): String =
    s"jdbc:mysql://google/${opts.getCloudSqlDb}?" +
      s"cloudSqlInstance=${opts.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"

}
