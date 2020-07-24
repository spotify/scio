package com.spotify.scio.extra.sparkey

import java.util

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapOf}
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.SCollection
import com.spotify.sparkey.{IndexHeader, LogHeader, SparkeyReader}
import org.apache.beam.sdk.options.PipelineOptions
import com.google.protobuf.ByteString


object SparkeyUriSideInput {
  object ReadP {
    def apply(options: PipelineOptions): ReadP = ReadP(() => RemoteFileUtil.create(options))
  }
  case class ReadP(rfuCreateFn: () => RemoteFileUtil)

  private trait TestSparkeyReader extends SparkeyReader {
    override def getAsString(key: String): String = ???
    override def getAsByteArray(key: Array[Byte]): Array[Byte] = ???
    override def getAsEntry(key: Array[Byte]): SparkeyReader.Entry = ???
    override def getIndexHeader: IndexHeader = ???
    override def getLogHeader: LogHeader = ???
    override def duplicate(): SparkeyReader = ???
    override def close(): Unit = ???
    override def iterator(): util.Iterator[SparkeyReader.Entry] = ???
  }

  def forTest(items: Map[String, String]): SparkeyUri = new SparkeyUri {
    override val basePath: String = "i should be ignored"
    override private[sparkey] def exists = true
    override def getReader: SparkeyReader = new TestSparkeyReader {
      override def getAsString(key: String): String = items.get(key).orNull
    }
  }

  def forTest(items: Seq[(Array[Byte], Array[Byte])]): SparkeyUri = new SparkeyUri {
    private val byteStrItems = items.map { case (k, v) => ByteString.copyFrom(k) -> v }.toMap
    override val basePath: String = "i should be ignored"
    override private[sparkey] def exists = true
    override def getReader: SparkeyReader = new TestSparkeyReader {
      override def getAsByteArray(key: Array[Byte]): Array[Byte] = {
        byteStrItems.get(ByteString.copyFrom(key)).orNull
      }
    }
  }
}

final case class SparkeyUriSideInput(path: String) extends ScioIO[SparkeyUri] {
  override def testId: String = s"SparkeyUriSideInput($path)"

  override type ReadP = SparkeyUriSideInput.ReadP
  override type WriteP = Nothing // ReadOnly

  final override val tapT = EmptyTapOf[SparkeyUri]
  override def tap(read: ReadP): Tap[tapT.T] = EmptyTap

  override protected def read(sc: ScioContext, params: ReadP): SCollection[SparkeyUri] = {
    sc.parallelize(Seq(SparkeyUri(path, params.rfuCreateFn)))
  }
  override protected def write(data: SCollection[SparkeyUri], params: WriteP): Tap[tapT.T] = {
    throw new UnsupportedOperationException("SparkeyUriSideInput is read-only")
  }
}
