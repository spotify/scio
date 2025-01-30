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
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.spotify.scio.coders.{Coder, CoderGrammar}
import com.spotify.scio.util.ScioUtil

import java.io.StringReader
import org.apache.beam.sdk.coders.RowCoder
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.{MatchResult, MetadataCoderV2, ResourceId, ResourceIdCoder}
import org.apache.beam.sdk.io.ReadableFileCoder
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.errorhandling.BadRecord
import org.apache.beam.sdk.transforms.windowing.{
  BoundedWindow,
  GlobalWindow,
  IntervalWindow,
  PaneInfo
}
import org.apache.beam.sdk.values.{KV, Row}

import scala.reflect.ClassTag

trait BeamTypeCoders extends CoderGrammar {
  import BeamTypeCoders._

  implicit lazy val intervalWindowCoder: Coder[IntervalWindow] = beam(IntervalWindow.getCoder)

  implicit lazy val globalWindowCoder: Coder[GlobalWindow] = beam(GlobalWindow.Coder.INSTANCE)

  implicit lazy val boundedWindowCoder: Coder[BoundedWindow] = kryo[BoundedWindow]

  implicit lazy val paneInfoCoder: Coder[PaneInfo] = beam(PaneInfo.PaneInfoCoder.of())

  def row(schema: BSchema): Coder[Row] = beam(RowCoder.of(schema))

  implicit def beamKVCoder[K: Coder, V: Coder]: Coder[KV[K, V]] = kv(Coder[K], Coder[V])

  implicit lazy val readableFileCoder: Coder[ReadableFile] = beam(ReadableFileCoder.of())

  implicit lazy val resourceIdCoder: Coder[ResourceId] = beam(ResourceIdCoder.of())

  implicit lazy val matchResultMetadataCoder: Coder[MatchResult.Metadata] =
    beam(MetadataCoderV2.of())

  implicit def genericJsonCoder[T <: GenericJson: ClassTag]: Coder[T] =
    xmap(Coder[String])(
      str => DefaultJsonObjectParser.parseAndClose(new StringReader(str), ScioUtil.classOf[T]),
      DefaultJsonObjectParser.getJsonFactory().toString(_)
    )

  // rely on serializable
  implicit val badRecordCoder: Coder[BadRecord] = kryo
  implicit val badRecordRecordCoder: Coder[BadRecord.Record] = kryo
  implicit val badRecordFailurCoder: Coder[BadRecord.Failure] = kryo
}

private[coders] object BeamTypeCoders extends BeamTypeCoders {
  private lazy val DefaultJsonObjectParser = new JsonObjectParser(GsonFactory.getDefaultInstance)
}
