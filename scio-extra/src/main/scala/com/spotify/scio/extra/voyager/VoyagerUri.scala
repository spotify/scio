package com.spotify.scio.extra.voyager

import com.spotify.scio.coders.Coder
import org.apache.flink.configuration.PipelineOptions

import java.io.File

trait VoyagerUri extends Serializable {
  val path: String
  private[voyager] def getReader(distanceMeasure: VoyagerDistanceMeasure, dim: Int): VoyagerReader
  private[voyager] def buildAndValidate(voyagerWriter: VoyagerWriter): Unit
  private[voyager] def exists: Boolean
}

private[voyager] object VoyagerUri {
  def apply(path: String, opts: PipelineOptions): VoyagerUri = ???
  implicit val voyagerUriCoder: Coder[VoyagerUri] = Coder.kryo[VoyagerUri]
}

private class LocalVoyagerUri(val path: String) extends VoyagerUri {
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    dim: Int
  ): VoyagerReader = ???

  override private[voyager] def buildAndValidate(voyagerWriter: VoyagerWriter): Unit = ???

  override private[voyager] def exists: Boolean = new File(path).exists()
}

private class RemoteVoyagerUri(val path: String, option: PipelineOptions) extends VoyagerUri {
  override private[voyager] def getReader(
    distanceMeasure: VoyagerDistanceMeasure,
    dim: Int
  ): VoyagerReader = ???

  override private[voyager] def buildAndValidate(voyagerWriter: VoyagerWriter): Unit = ???

  override private[voyager] def exists: Boolean = ???
}

private[voyager] class VoyagerWriter(
  distanceMeasure: VoyagerDistanceMeasure,
  dim: Int,
  ef: Int,
  m: Int
) {}
