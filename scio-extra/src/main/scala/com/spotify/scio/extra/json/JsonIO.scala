/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.extra.json

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TextIO}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import io.circe.Printer
import io.circe.parser._
import io.circe.syntax._
import org.apache.beam.sdk.{io => beam}

import scala.reflect.ClassTag
import scala.util.{Left, Right}

final case class JsonIO[T: ClassTag: Encoder: Decoder: Coder](path: String) extends ScioIO[T] {

  override type ReadP = Unit
  override type WriteP = JsonIO.WriteParam
  override final val tapT = TapOf[T]

  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.wrap(sc.applyInternal(beam.TextIO.read().from(path))).map(decodeJson)

  override def write(data: SCollection[T], params: WriteP): Tap[T] = {
    data
      .map(x => params.printer.pretty(x.asJson))
      .applyInternal(jsonOut(path, params))
    tap(())
  }

  override def tap(params: ReadP): Tap[T] = new Tap[T] {
    override def value: Iterator[T] =
      TextIO.textFile(ScioUtil.addPartSuffix(path)).map(decodeJson)
    override def open(sc: ScioContext): SCollection[T] =
      JsonIO(ScioUtil.addPartSuffix(path)).read(sc, ())
  }

  private def decodeJson(json: String): T = decode[T](json) match {
    case Left(e)  => throw e
    case Right(t) => t
  }

  private def jsonOut(path: String, params: WriteP) =
    beam.TextIO
      .write()
      .to(pathWithShards(path))
      .withSuffix(params.suffix)
      .withNumShards(params.numShards)
      .withWritableByteChannelFactory(
        beam.FileBasedSink.CompressionType.fromCanonical(params.compression))

  private[scio] def pathWithShards(path: String) =
    path.replaceAll("\\/+$", "") + "/part"

}

object JsonIO {
  final case class WriteParam(suffix: String = ".json",
                              numShards: Int = 0,
                              compression: beam.Compression = beam.Compression.UNCOMPRESSED,
                              printer: Printer = Printer.noSpaces)
}
