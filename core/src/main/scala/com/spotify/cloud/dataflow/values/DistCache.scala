package com.spotify.cloud.dataflow.values

import java.io.{FileOutputStream, File}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, GcsOptions}
import com.google.cloud.dataflow.sdk.util.GcsUtil
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath

import scala.util.Random

sealed trait DistCache[F] extends Serializable {

  @transient protected var file: F = _

  protected def init(): Unit

  protected def fetch(uri: URI, prefix: String, gcsUtil: GcsUtil): File = {
    val path = prefix + uri.getPath.split("/").last
    val file = new File(path)

    if (!file.exists()) {
      val fos: FileOutputStream = new FileOutputStream(path)
      val dst = fos.getChannel
      val src = gcsUtil.open(GcsPath.fromUri(uri))

      dst.transferFrom(src, 0, src.size())
      dst.close()
      fos.close()
    }

    file
  }

  def get(): F = {
    if (file == null) init()
    file
  }

}

private[dataflow] class MockDistCache[F](val value: F) extends DistCache[F] {
  override protected def init(): Unit = {}
  override def get(): F = value
}

private[dataflow] class DistCacheSingle[F](val uri: URI, val initFn: File => F, gcsOptions: GcsOptions)
  extends DistCache[F] {

  private val json = new ObjectMapper().writeValueAsString(gcsOptions)

  override protected def init(): Unit = {
    val opts: GcsOptions = new ObjectMapper().readValue(json, classOf[PipelineOptions]).as(classOf[GcsOptions])
    val prefix = sys.props("java.io.tmpdir") + "/" + opts.getAppName + "_" + Random.nextInt()
    val gcsUtil = opts.getGcsUtil
    file = initFn(fetch(uri, prefix, gcsUtil))
  }

}

private[dataflow] class DistCacheMulti[F](val uris: Seq[URI], val initFn: Seq[File] => F, gcsOptions: GcsOptions)
  extends DistCache[F] {

  private val json = new ObjectMapper().writeValueAsString(gcsOptions)

  override protected def init(): Unit = {
    val opts: GcsOptions = new ObjectMapper().readValue(json, classOf[PipelineOptions]).as(classOf[GcsOptions])
    val prefix = sys.props("java.io.tmpdir") + "/" + opts.getAppName + Random.nextInt()
    val gcsUtil = opts.getGcsUtil
    val localFiles = uris.map(uri => fetch(uri, prefix, gcsUtil))
    file = initFn(localFiles)
  }

}


