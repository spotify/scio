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

package com.spotify.scio.iceberg.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.iceberg.IcebergIO
import com.spotify.scio.managed.ManagedIO
import com.spotify.scio.values.SCollection
import magnolify.beam.RowType


class IcebergScioContextSyntax(self: ScioContext) {
  def iceberg[T : Coder](config: Map[String, AnyRef])(implicit rt: RowType[T]): SCollection[T] =
    self.read(IcebergIO(config))(ManagedIO.ReadParam(rt.schema))

  /**
   * @see [[org.apache.beam.sdk.io.iceberg.SchemaTransformConfiguration SchemaTransformConfiguration]]
   */
  def iceberg[T : Coder](table: String, catalogName: String = null, catalogProperties: Map[String, String] = null, configProperties: Map[String, String] = null)(implicit rt: RowType[T]): SCollection[T] = {
    // see
    val config = Map[String, AnyRef](
      "table" -> table,
      "catalog_name" -> catalogName,
      "catalog_properties" -> catalogProperties,
      "config_properties" -> configProperties
    ).filter(_._2 != null)
    iceberg(config)
  }
}

trait ScioContextSyntax {
  implicit def icebergScioContextSyntax(self: ScioContext): IcebergScioContextSyntax =
    new IcebergScioContextSyntax(self)
}
