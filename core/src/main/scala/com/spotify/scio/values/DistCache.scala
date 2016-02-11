package com.spotify.scio.values

import java.io.{File, FileOutputStream}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.dataflow.sdk.options.{GcsOptions, PipelineOptions}
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.util.ScioUtil
import org.slf4j.{Logger, LoggerFactory}

/** Encapsulate files on Google Cloud Storage that can be distributed to all workers. */
sealed trait DistCache[F] extends Serializable {
  /** Extract the underlying data. */
  def apply(): F
}

private[scio] abstract class FileDistCache[F](options: GcsOptions) extends DistCache[F] {

  override def apply(): F = data

  protected def init(): F

  protected lazy val data: F = init()

  private val logger: Logger = LoggerFactory.getLogger(classOf[DistCache[_]])

  // Serialize options to avoid shipping it with closure
  private val json: String = new ObjectMapper().writeValueAsString(options)
  private def opts: GcsOptions = new ObjectMapper().readValue(json, classOf[PipelineOptions]).as(classOf[GcsOptions])

  private def fetchFromGCS(uri: URI, prefix: String): File = {
    val path = prefix + uri.getPath.split("/").last
    val file = new File(path)

    if (!file.exists()) {
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

  private def temporaryPrefix(uris: Seq[URI]): String = {
    val hash = Hashing.sha1().hashString(uris.map(_.toString).mkString("|"), Charsets.UTF_8)
    sys.props("java.io.tmpdir") + "/" + hash.toString.substring(0, 8) + "-"
  }

  protected def prepareFiles(uris: Seq[URI]): Seq[File] = {
    if (classOf[DirectPipelineRunner] isAssignableFrom opts.getRunner) {
      uris.map(u => new File(u.toString))
    } else {
      val p = temporaryPrefix(uris)
      uris.map(fetchFromGCS(_, p))
    }
  }

  protected def verifyUri(uri: URI): Unit = {
    if (classOf[DirectPipelineRunner] isAssignableFrom opts.getRunner) {
      require(ScioUtil.isLocalUri(uri), s"Not a local path $uri")
    } else {
      require(ScioUtil.isGcsUri(uri), s"Not a GCS path $uri")
    }
  }

}

private[scio] class MockDistCache[F](val value: F) extends DistCache[F] {
  override def apply(): F = value
}

private[scio] class DistCacheSingle[F](val uri: URI, val initFn: File => F, options: GcsOptions)
  extends FileDistCache[F](options) {

  verifyUri(uri)

  override protected def init(): F = initFn(prepareFiles(Seq(uri)).head)
}

private[scio] class DistCacheMulti[F](val uris: Seq[URI], val initFn: Seq[File] => F, options: GcsOptions)
  extends FileDistCache[F](options) {

  uris.foreach(verifyUri)

  override protected def init(): F = initFn(prepareFiles(uris))
}
