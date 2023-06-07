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
import com.spotify.scio.io.TapT
import com.spotify.scio.util.FilenamePolicySupplier
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment

final case class JsonIO[T: Encoder: Decoder: Coder](path: String) extends ScioIO[T] {
  override type ReadP = JsonIO.ReadParam
  override type WriteP = JsonIO.WriteParam
  final override val tapT: TapT.Aux[T, T] = TapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(TextIO(path))(TextIO.ReadParam(
      compression = params.compression,
      suffix = params.suffix,
      emptyMatchTreatment = params.emptyMatchTreatment
    ))
      .map(decodeJson)

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    data
      .map(x => params.printer.print(x.asJson))
      .write(TextIO(path))(
        TextIO.WriteParam(
          params.suffix,
          params.numShards,
          params.compression,
          None,
          None,
          params.filenamePolicySupplier,
          params.prefix,
          params.shardNameTemplate,
          params.tempDirectory
        )
      )
    tap(JsonIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[T] = new Tap[T] {
    override def value: Iterator[T] = {
      val filePattern = ScioUtil.filePattern(path, params.suffix)
      TextIO.textFile(filePattern).map(decodeJson)
    }

    override def open(sc: ScioContext): SCollection[T] =
      JsonIO(path).read(sc, params)
  }

  private def decodeJson(json: String): T =
    decode[T](json).fold(throw _, identity)
}

object JsonIO {

  object ReadParam {
    val DefaultCompression: Compression = beam.Compression.AUTO
    val DefaultSuffix: String = null
    val DefaultEmptyMatchTreatment: EmptyMatchTreatment = EmptyMatchTreatment.DISALLOW


    private[scio] def apply(params: WriteParam): ReadParam = new ReadParam(
      params.compression,
      params.suffix + params.compression.getSuggestedSuffix
    )
  }

  final case class ReadParam private (
    compression: beam.Compression = ReadParam.DefaultCompression,
    suffix: String = ReadParam.DefaultSuffix,
    emptyMatchTreatment: EmptyMatchTreatment = ReadParam.DefaultEmptyMatchTreatment
  )

  object WriteParam {
    val DefaultNumShards: Int = 0
    val DefaultSuffix: String = ".json"
    val DefaultCompression: Compression = Compression.UNCOMPRESSED
    val DefaultPrinter: Printer = Printer.noSpaces
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultShardNameTemplate: String = null
    val DefaultPrefix: String = null
    val DefaultTempDirectory: String = null
  }

  final case class WriteParam private (
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    printer: Printer = WriteParam.DefaultPrinter,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )
}
