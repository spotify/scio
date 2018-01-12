package com.spotify.scio.nio

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{FileStorage, Tap}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{Compression, FileBasedSink, TextIO => BTextIO}

import scala.concurrent.Future

case class TextIO(name: String, path: String) extends ScioIO[String] with Tap[String] {

  case class ReadParams(compression: Compression = Compression.AUTO)

  case class WriteParams(suffix: String = ".txt",
                         numShards: Int = 0,
                         compression: Compression = Compression.UNCOMPRESSED)

  type ReadP = ReadParams
  type WriteP = WriteParams

  def read(sc: ScioContext, params: ReadParams): SCollection[String] = sc.requireNotClosed {
    if(sc.isTest) {
      // TODO: support test
      throw new UnsupportedOperationException("TextIO test is not yet supported")
    } else {
      sc.wrap(sc.applyInternal(BTextIO.read().from(path)
        .withCompression(params.compression))).setName(name)
    }
  }

  def write(pipeline: SCollection[String], params: WriteParams): Future[Tap[String]] = {
    if (pipeline.context.isTest) {
      // TODO: support test
      throw new UnsupportedOperationException("TextIO test is not yet supported")
    } else {
      pipeline.applyInternal(textOut(path, params))
      pipeline.context.makeFuture(TextIO(name, ScioUtil.addPartSuffix(path)))
    }
  }

  /** Read data set into memory. */
  def value: Iterator[String] = FileStorage(path).textFile

  /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
  def open(sc: ScioContext): SCollection[String] = read(sc, ReadParams())

  private[scio] def textOut(path: String,
                            params: WriteParams) = {
    BTextIO.write()
      .to(pathWithShards(path))
      .withSuffix(params.suffix)
      .withNumShards(params.numShards)
      .withWritableByteChannelFactory(
        FileBasedSink.CompressionType.fromCanonical(params.compression))
  }

  private[scio] def pathWithShards(path: String) = path.replaceAll("\\/+$", "") + "/part"

}

