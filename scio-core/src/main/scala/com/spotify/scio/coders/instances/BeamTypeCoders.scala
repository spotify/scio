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

package com.spotify.scio.coders.instances

import com.google.api.client.json.GenericJson
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.json.JsonObjectParser
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.ScioUtil
import java.io.StringReader
import org.apache.beam.sdk.coders.RowCoder
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.{MatchResult, MetadataCoderV2}
import org.apache.beam.sdk.io.ReadableFileCoder
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.beam.sdk.values.{KV, Row}
import scala.reflect.ClassTag

trait BeamTypeCoders {
  import BeamTypeCoders._

  implicit def intervalWindowCoder: Coder[IntervalWindow] = Coder.beam(IntervalWindow.getCoder)

  implicit def boundedWindowCoder: Coder[BoundedWindow] = Coder.kryo[BoundedWindow]

  implicit def paneInfoCoder: Coder[PaneInfo] = Coder.beam(PaneInfo.PaneInfoCoder.of())

  def row(schema: BSchema): Coder[Row] = Coder.beam(RowCoder.of(schema))

  implicit def beamKVCoder[K: Coder, V: Coder]: Coder[KV[K, V]] = Coder.kv(Coder[K], Coder[V])

  implicit def readableFileCoder: Coder[ReadableFile] = Coder.beam(ReadableFileCoder.of())

  implicit def matchResultMetadataCoder: Coder[MatchResult.Metadata] =
    Coder.beam(MetadataCoderV2.of())

  implicit def genericJsonCoder[T <: GenericJson: ClassTag]: Coder[T] =
    Coder.xmap(Coder[String])(
      str => DefaultJsonObjectParser.parseAndClose(new StringReader(str), ScioUtil.classOf[T]),
      DefaultJsonObjectParser.getJsonFactory().toString(_)
    )
}

private[coders] object BeamTypeCoders extends BeamTypeCoders {
  private lazy val DefaultJsonObjectParser = new JsonObjectParser(new JacksonFactory)
}
