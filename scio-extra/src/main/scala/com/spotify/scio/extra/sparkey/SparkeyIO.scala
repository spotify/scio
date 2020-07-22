package com.spotify.scio.extra.sparkey

import java.util

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapOf}
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.SCollection
import com.spotify.sparkey.{IndexHeader, LogHeader, SparkeyReader}
import org.apache.beam.sdk.options.PipelineOptions

final case class SparkeyUriTap(private val singleSparkeyUri: SparkeyUri) extends Tap[SparkeyUri] {
  override def value: Iterator[SparkeyUri] = List(singleSparkeyUri).toIterator
  override def open(sc: ScioContext): SCollection[SparkeyUri] = sc.parallelize(value.toIterable)
}

object SparkeyIO {
  object ReadP {
    def apply(options: PipelineOptions): ReadP = ReadP(() => RemoteFileUtil.create(options))
  }
  case class ReadP(rfuCreateFn: () => RemoteFileUtil)

  def testUri(in: List[(String, String)]): SparkeyUri = {
    new SparkeyUri {
      private val items = in.toMap
      override val basePath: String = "i should be ignored"
      override private[sparkey] def exists = true
      override def getReader: SparkeyReader = {
        new SparkeyReader {
          override def getAsString(key: String): String = items.get(key).orNull
          override def getAsByteArray(key: Array[Byte]): Array[Byte] = ???
          override def getAsEntry(key: Array[Byte]): SparkeyReader.Entry = ???
          override def getIndexHeader: IndexHeader = ???
          override def getLogHeader: LogHeader = ???
          override def duplicate(): SparkeyReader = ???
          override def close(): Unit = ???
          override def iterator(): util.Iterator[SparkeyReader.Entry] = ???
        }
      }
    }
  }
}

final case class SparkeyIO(path: String) extends ScioIO[SparkeyUri] {
  override type ReadP = SparkeyIO.ReadP
  override type WriteP = Nothing // ReadOnly

  final override val tapT = TapOf[SparkeyUri]
  override def testId: String = s"SparkeyIO($path)"

  override def tap(read: ReadP): Tap[SparkeyUri] = SparkeyUriTap(SparkeyUri(path, read.rfuCreateFn))
  override protected def write(data: SCollection[SparkeyUri], params: WriteP): Tap[SparkeyUri] =
    ??? // ReadOnly
  override protected def read(sc: ScioContext, params: ReadP): SCollection[SparkeyUri] =
    sc.parallelize(Seq(SparkeyUri(path, params.rfuCreateFn)))
}
