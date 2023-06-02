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
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior
import org.apache.beam.sdk.io.jdbc.{JdbcIO => BJdbcIO}
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.display.DisplayData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

case class Foo(field: String)

class JdbcIOTests extends AnyFlatSpec with Matchers {

  private val ReadQueryId = DisplayData.Identifier.of(
    DisplayData.Path.root(),
    classOf[BJdbcIO.Read[_]],
    "query"
  )

  private val WriteStatementId = "[fn]class org.apache.beam.sdk.io.jdbc.JdbcIO$WriteFn:statement"

  it must "add to pipeline overridden Read transform" in {
    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val sc = ScioContext(opts)
    sc.jdbcSelect[String](
      getConnectionOptions(opts),
      "initial query",
      configOverride = (x: BJdbcIO.Read[String]) => x.withQuery("overridden query")
    ) { (rs: ResultSet) =>
      rs.getString(1)
    }

    val transform = getPipelineTransforms(sc).collect { case t: BJdbcIO.Read[String] => t }.head
    val displayData = DisplayData.from(transform).asMap().asScala

    displayData should contain key ReadQueryId
    displayData(ReadQueryId).getValue should be("overridden query")
  }

  it must "add to pipeline overridden Write transform" in {
    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val sc = ScioContext(opts)

    sc.parallelize(List("1", "2", "3"))
      .saveAsJdbc(
        getConnectionOptions(opts),
        "INSERT INTO <this> VALUES( ?, ? ..?)",
        configOverride = _.withStatement("updated statement")
      ) { (_, _) => }

    val transform = getPipelineTransforms(sc).filter(x => x.toString.contains("WriteFn")).head
    val displayData =
      DisplayData.from(transform).asMap().asScala.map { case (k, v) => (k.toString, v) }

    displayData should contain key WriteStatementId
    displayData(WriteStatementId).getValue should be("updated statement")
  }

  private def getPipelineTransforms(sc: ScioContext): Iterable[PTransform[_, _]] = {
    val actualTransforms = new ArrayBuffer[PTransform[_, _]]()
    sc.pipeline.traverseTopologically(new PipelineVisitor.Defaults {
      override def enterCompositeTransform(
        node: TransformHierarchy#Node
      ): PipelineVisitor.CompositeBehavior = {
        if (node.getTransform != null) {
          actualTransforms.append(node.getTransform)
        }
        CompositeBehavior.ENTER_TRANSFORM
      }

      override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
        if (node.getTransform != null) {
          actualTransforms.append(node.getTransform)
        }
    })
    actualTransforms
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
