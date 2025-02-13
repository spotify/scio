/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.managed

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.managed.Managed
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.{PCollectionRowTuple, Row}
import scala.jdk.CollectionConverters._

final case class ManagedIO(ioName: String, config: Map[String, Object]) extends ScioIO[Row] {
  override type ReadP = ManagedIO.ReadParam
  override type WriteP = ManagedIO.WriteParam
  override val tapT: TapT.Aux[Row, Nothing] = EmptyTapOf[Row]

  private lazy val _config: java.util.Map[String, Object] = {
    // recursively convert this yaml-compatible nested scala map to java map
    // we either do this or the user has to create nested java maps in scala code
    // both are bad
    def _convert(a: Object): Object = {
      a match {
        case m: Map[_, _] =>
          m.asInstanceOf[Map[_, Object]].map { case (k, v) => k -> _convert(v) }.asJava
        case i: Iterable[_] => i.map(o => _convert(o.asInstanceOf[Object])).asJava
        case _              => a
      }
    }
    config.map { case (k, v) => k -> _convert(v) }.asJava
  }

  // not-ideal IO naming, but we have no identifier except the config map
  override def testId: String = s"ManagedIO($ioName, ${config.toString})"
  override protected def read(sc: ScioContext, params: ManagedIO.ReadParam): SCollection[Row] = {
    sc.wrap(
      sc.applyInternal[PCollectionRowTuple](
        Managed.read(ioName).withConfig(_config)
      ).getSinglePCollection
    )
  }

  override protected def write(
    data: SCollection[Row],
    params: ManagedIO.WriteParam
  ): Tap[tapT.T] = {
    data.applyInternal(Managed.write(ioName).withConfig(_config))
    EmptyTap
  }

  override def tap(read: ManagedIO.ReadParam): Tap[tapT.T] = EmptyTap
}

object ManagedIO {
  final case class ReadParam(schema: Schema)
  type WriteParam = Unit
}
