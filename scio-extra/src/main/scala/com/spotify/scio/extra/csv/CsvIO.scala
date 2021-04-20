/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.extra.csv

import java.io.{Reader, Writer}
import java.nio.channels.{Channels, WritableByteChannel}
import java.nio.charset.StandardCharsets

import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import kantan.csv._
import kantan.codecs.compat._ // scalafix:ok
import kantan.csv.CsvConfiguration.{Header, QuotePolicy}
import kantan.csv.engine.ReaderEngine
import kantan.csv.ops._
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.PCollection

/**
 * This package uses a CSV mapper called [[https://nrinaudo.github.io/kantan.csv/ Kantan]].
 *
 * import the following:
 * {{{
 *   import kantan.csv._
 *   import com.spotify.scio.extra.csv._
 * }}}
 *
 * =Reading=
 * == with a header ==
 * [[https://nrinaudo.github.io/kantan.csv/headers.html Kantan docs]]
 * {{{
 *  case class User(name: String, age: Int)
 *  implicit val decoder = HeaderDecoder.decoder("fullName", "userAge")(User.apply _)
 *  val users: SCollection[User] = scioContext.csvFile(path)
 * }}}
 *
 * == without a header ==
 * [[https://nrinaudo.github.io/kantan.csv/rows_as_arbitrary_types.html Kantan docs]]
 * {{{
 *  case class User(name: String, age: Int)
 *  implicit val decoder = RowDecoder.ordered { (name: String, age: Int) => User(name, age) }
 *  val csvConfiguration = CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfig.withoutHeader)
 *  val users: SCollection[User] = scioContext.csvFile(path, csvConfiguration)
 * }}}
 *
 * =Writing=
 * == with a header ==
 * [[https://nrinaudo.github.io/kantan.csv/headers.html Kantan docs]]
 * {{{
 *   case class User(name: String, age: Int)
 *   implicit val encoder = HeaderEncoder.caseEncoder("fullName", "age")(User.unapply)
 *
 *   val users: SCollection[User] = ???
 *   users.saveAsCsvFile(path)
 * }}}
 * == without a header ==
 * [[https://nrinaudo.github.io/kantan.csv/arbitrary_types_as_rows.html Kantan docs]]
 * {{{
 *   case class User(name: String, age: Int)
 *   implicit val encoder = RowEncoder.encoder(0, 1)((u: User) => (u.name, u.age))
 *
 *   val users: SCollection[User] = ???
 *   users.saveAsCsvFile(path)
 * }}}
 */
object CsvIO {

  final val DefaultCsvConfig: CsvConfiguration = CsvConfiguration(
    cellSeparator = ',',
    quote = '"',
    quotePolicy = QuotePolicy.WhenNeeded,
    header = Header.Implicit
  )
  final val DefaultReadParams: ReadParam = CsvIO.ReadParam(compression = beam.Compression.AUTO)
  final val DefaultWriteParams: WriteParam =
    CsvIO.WriteParam(compression = beam.Compression.UNCOMPRESSED)
  final val DefaultFileSuffix = ".csv"

  final case class ReadParam(
    compression: beam.Compression = beam.Compression.AUTO,
    csvConfiguration: CsvConfiguration = DefaultCsvConfig
  )

  final case class WriteParam(
    compression: beam.Compression = beam.Compression.UNCOMPRESSED,
    csvConfiguration: CsvConfiguration = DefaultCsvConfig,
    suffix: String = DefaultFileSuffix,
    numShards: Int = 1
  ) {
    def toReadParam: ReadParam =
      ReadParam(compression = compression, csvConfiguration = csvConfiguration)
  }

  final case class Read[T: HeaderDecoder: Coder](path: String) extends ScioIO[T] {
    override type ReadP = CsvIO.ReadParam
    override type WriteP = CsvIO.WriteParam
    final override val tapT: TapT.Aux[T, T] = TapOf[T]

    override protected def read(
      sc: ScioContext,
      params: ReadParam = CsvIO.DefaultReadParams
    ): SCollection[T] = CsvIO.read(sc, path, params)

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] =
      throw new UnsupportedOperationException("Use CsvIO.Write() for writing")

    override def tap(params: ReadP): Tap[T] = new CsvTap[T](path, params)
  }

  final case class Write[T: HeaderEncoder](path: String) extends ScioIO[T] {
    override type ReadP = Nothing // WriteOnly
    override type WriteP = CsvIO.WriteParam
    final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      throw new UnsupportedOperationException("Use CsvIO.Read() for reading")

    override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
      data.applyInternal(csvOut(path, params))
      EmptyTap
    }

    override def tap(params: ReadP): Tap[Nothing] = EmptyTap
  }

  final case class ReadWrite[T: HeaderCodec: Coder](path: String) extends ScioIO[T] {
    override type ReadP = CsvIO.ReadParam
    override type WriteP = CsvIO.WriteParam
    final override val tapT: TapT.Aux[T, T] = TapOf[T]

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      CsvIO.read(sc, path, params)

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
      data.applyInternal(csvOut(path, params))
      tap(params.toReadParam)
    }

    override def tap(params: ReadP): Tap[T] = new CsvIO.CsvTap[T](path, params)
  }

  private def csvOut[T: HeaderEncoder](path: String, params: WriteParam): FileIO.Write[Void, T] =
    beam.FileIO
      .write()
      .to(path)
      .withSuffix(params.suffix)
      .withNumShards(params.numShards)
      .withCompression(params.compression)
      .via(new CsvSink(params.csvConfiguration))

  private def read[T: HeaderDecoder: Coder](sc: ScioContext, path: String, params: ReadParam) =
    sc.parallelize(Seq(path))
      .withName("Read CSV")
      .readFiles(
        ParDo
          .of(CsvIO.ReadDoFn[T](params.csvConfiguration))
          .asInstanceOf[PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[T]]],
        DirectoryTreatment.PROHIBIT,
        params.compression
      )

  final private class CsvTap[T: HeaderDecoder: Coder](path: String, params: ReadParam)
      extends Tap[T] {
    override def value: Iterator[T] =
      BinaryIO
        .openInputStreamsFor(ScioUtil.addPartSuffix(path))
        .flatMap(_.asUnsafeCsvReader[T](params.csvConfiguration).iterator)

    override def open(sc: ScioContext): SCollection[T] = CsvIO.read(sc, path, params)
  }

  final private[scio] case class ReadDoFn[T: HeaderDecoder](
    config: CsvConfiguration,
    charSet: String = StandardCharsets.UTF_8.name()
  ) extends DoFn[ReadableFile, T] {

    @ProcessElement
    def process(pc: ProcessContext): Unit = {
      val reader: Reader = Channels.newReader(pc.element().open(), charSet)
      implicit val engine: ReaderEngine = ReaderEngine.internalCsvReaderEngine
      reader
        .asUnsafeCsvReader[T](config)
        .foreach(pc.output)
    }
  }

  final private class CsvSink[T: HeaderEncoder](csvConfig: CsvConfiguration)
      extends FileIO.Sink[T] {
    var csvWriter: CsvWriter[T] = _
    var byteChannelWriter: Writer = _

    override def open(channel: WritableByteChannel): Unit = {
      byteChannelWriter = Channels.newWriter(channel, StandardCharsets.UTF_8.name())
      csvWriter = byteChannelWriter.asCsvWriter[T](csvConfig)
    }

    override def write(element: T): Unit = {
      csvWriter.write(element)
      ()
    }

    override def flush(): Unit =
      byteChannelWriter.flush()
  }

}
