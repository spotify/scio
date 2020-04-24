/*
 * Copyright 2019 Spotify AB.
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

final case class JsonIO[T: Encoder: Decoder: Coder](path: String) extends ScioIO[T] {
  override type ReadP = JsonIO.ReadParam
  override type WriteP = JsonIO.WriteParam
  final override val tapT = TapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(TextIO(path))(TextIO.ReadParam(params.compression)).map(decodeJson)

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    data
      .map(x => params.printer.print(x.asJson))
      .write(TextIO(path))(TextIO.WriteParam(params.suffix, params.numShards, params.compression))
    tap(JsonIO.ReadParam(params.compression))
  }

  override def tap(params: ReadP): Tap[T] = new Tap[T] {
    override def value: Iterator[T] =
      TextIO.textFile(ScioUtil.addPartSuffix(path)).map(decodeJson)
    override def open(sc: ScioContext): SCollection[T] =
      JsonIO(ScioUtil.addPartSuffix(path)).read(sc, params)
  }

  private def decodeJson(json: String): T =
    decode[T](json).fold(throw _, identity)
}

object JsonIO {
  final case class ReadParam(compression: beam.Compression = beam.Compression.AUTO)
  final case class WriteParam(
    suffix: String = ".json",
    numShards: Int = 0,
    compression: beam.Compression = beam.Compression.UNCOMPRESSED,
    printer: Printer = Printer.noSpaces
  )
}
