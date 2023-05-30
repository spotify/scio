package com.spotify.scio.jdbc

import com.spotify.scio.ScioContext
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior
import org.apache.beam.sdk.io.jdbc.{JdbcIO => BJdbcIO}
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.transforms.PTransform
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer

class JdbcIOTests3 extends AnyFlatSpec with Matchers {

  it must "add to pipeline overridden Read transform" in {
    val args = Array[String]()
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val sc = ScioContext(opts)
    var expectedTransform: BJdbcIO.Read[String] = null
    sc.jdbcSelect[String](
      getDefaultReadOptions(opts).copy(configOverride = r => {
        expectedTransform = r.withQuery("overridden query")
        expectedTransform
      })
    )

    expectedTransform should not be null
    getPipelineTransforms(sc) should contain(expectedTransform)
  }

  private def getPipelineTransforms(sc: ScioContext): Iterable[PTransform[_, _]] = {
    val actualTransforms = new ArrayBuffer[PTransform[_, _]]()
    sc.pipeline.traverseTopologically(new PipelineVisitor.Defaults {
      override def enterCompositeTransform(
        node: TransformHierarchy#Node
      ): PipelineVisitor.CompositeBehavior = {
        actualTransforms.addOne(node.getTransform)
        CompositeBehavior.ENTER_TRANSFORM
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

  def getWriteOptions(opts: CloudSqlOptions): JdbcWriteOptions[String] =
    JdbcWriteOptions[String](
      connectionOptions = getConnectionOptions(opts),
      statement = "INSERT INTO <this> VALUES( ?, ? ..?)"
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
