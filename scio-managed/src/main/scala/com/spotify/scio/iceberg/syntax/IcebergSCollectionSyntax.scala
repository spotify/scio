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

import com.spotify.scio.coders.Coder
import com.spotify.scio.iceberg.IcebergIO
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import magnolify.beam.RowType

class IcebergSCollectionSyntax[T: RowType: Coder](self: SCollection[T]) {
  /**
   * @see
   *   [[org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider IcebergWriteSchemaTransformProvider]]
   * https://github.com/apache/beam/blob/v2.68.0/sdks/java/io/iceberg/src/main/java/org/apache/beam/sdk/io/iceberg/IcebergWriteSchemaTransformProvider.java#L135-L153
   */
  def saveAsIceberg(
    table: String,
    catalogName: String = null,
    catalogProperties: Map[String, String] = IcebergIO.WriteParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = IcebergIO.WriteParam.DefaultConfigProperties,
    triggeringFrequencySeconds: Int = IcebergIO.WriteParam.DefaultTriggeringFrequencySeconds,
    directWriteByteLimit: Int = IcebergIO.WriteParam.DefaultDirectWriteByteLimit,
    keep: List[String] = IcebergIO.WriteParam.DefaultKeep,
    drop: List[String] = IcebergIO.WriteParam.DefaultDrop,
    only: String = IcebergIO.WriteParam.DefaultOnly
  ): ClosedTap[Nothing] = {
    val params = IcebergIO.WriteParam(catalogProperties, configProperties, triggeringFrequencySeconds,
      directWriteByteLimit, keep, drop, only)
    self.write(IcebergIO(table, Option(catalogName)))(params)
  }
}

trait SCollectionSyntax {
  implicit def icebergSCollectionSyntax[T: RowType: Coder](
    self: SCollection[T]
  ): IcebergSCollectionSyntax[T] =
    new IcebergSCollectionSyntax(self)
}
