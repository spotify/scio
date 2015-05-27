package com.spotify.cloud.dataflow.values

import java.io.{FileOutputStream, File}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, GcsOptions}
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.slf4j.{Logger, LoggerFactory}

/** Encapsulate files on Google Cloud Storage that can be distributed to all workers. */
sealed trait DistCache[F] extends Serializable {
  /** Extract the underlying data. */
  def apply(): F
}

private[dataflow] abstract class GcsDistCache[F](gcsOptions: GcsOptions) extends DistCache[F] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DistCache[_]])

  protected val json: String = new ObjectMapper().writeValueAsString(gcsOptions)

  protected lazy val data: F = init()

  override def apply(): F = data

  protected def init(): F

  protected def fetch(uri: URI, prefix: String): File = {
    val path = prefix + uri.getPath.split("/").last
    val file = new File(path)

    if (!file.exists()) {
      val opts: GcsOptions = new ObjectMapper().readValue(json, classOf[PipelineOptions]).as(classOf[GcsOptions])
      val gcsUtil = opts.getGcsUtil

      val fos: FileOutputStream = new FileOutputStream(path)
      val dst = fos.getChannel
      val src = gcsUtil.open(GcsPath.fromUri(uri))

      val size = dst.transferFrom(src, 0, src.size())
      logger.info(s"DistCache $uri fetched to $path, size: $size")

      dst.close()
      fos.close()
    } else {
      logger.info(s"DistCache $uri already fetched ")
    }

    file
  }

  protected def prefix(uris: URI*): String = {
    val hash = Hashing.sha1().hashString(uris.map(_.toString).mkString("|"), Charsets.UTF_8)
    sys.props("java.io.tmpdir") + "/" + hash.toString.substring(0, 8) + "-"
  }

}

private[dataflow] class MockDistCache[F](val value: F) extends DistCache[F] {
  override def apply(): F = value
}

private[dataflow] class DistCacheSingle[F](val uri: URI, val initFn: File => F, gcsOptions: GcsOptions)
  extends GcsDistCache[F](gcsOptions) {
  override protected def init(): F = initFn(fetch(uri, prefix(uri)))
}

private[dataflow] class DistCacheMulti[F](val uris: Seq[URI], val initFn: Seq[File] => F, gcsOptions: GcsOptions)
  extends GcsDistCache[F](gcsOptions) {
  override protected def init(): F = initFn(uris.map(uri => fetch(uri, prefix(uris: _*))))
}
