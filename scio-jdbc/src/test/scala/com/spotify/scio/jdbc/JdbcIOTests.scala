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
      getDefaultReadOptions(opts).copy(configOverride = _.withQuery("overridden query"))
    )

    val transform = getPipelineTransforms(sc).collect { case t: BJdbcIO.Read[String] => t }.head
    val displayData = DisplayData.from(transform).asMap().asScala

    displayData should contain key ReadQueryId
    displayData(ReadQueryId).getValue should be("overridden query")
  }

  it must "add to pipeline overridden Write transform" in {

    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val sc = ScioContext(opts)

    def improveWrite(statement: BJdbcIO.Write[String]) =
      statement.withStatement("updated statement")

    sc.parallelize(List("1", "2", "3"))
      .saveAsJdbc(getDefaultWriteOptions[String](opts).copy(configOverride = improveWrite))

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
          actualTransforms.addOne(node.getTransform)
        }
        CompositeBehavior.ENTER_TRANSFORM
      }

      override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
        if (node.getTransform != null) {
          actualTransforms.addOne(node.getTransform)
        }
    })
    actualTransforms
  }

  def getDefaultReadOptions(opts: CloudSqlOptions): JdbcReadOptions[String] =
    JdbcReadOptions(
      connectionOptions = getConnectionOptions(opts),
      query = "SELECT <this> FROM <this>",
      rowMapper = (rs: ResultSet) => rs.getString(1)
    )

  def getDefaultWriteOptions[T](opts: CloudSqlOptions): JdbcWriteOptions[T] =
    JdbcWriteOptions[T](
      connectionOptions = getConnectionOptions(opts),
      statement = "INSERT INTO <this> VALUES( ?, ? ..?)",
      preparedStatementSetter = (_, _) => {}
    )

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
