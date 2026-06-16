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
   * Writes to an Iceberg table using Beam's [[org.apache.beam.sdk.managed.Managed]] IO.
   *
   * @param table
   *   the unique identifier for the Iceberg table, in the format `{table_namespace}.{table_name}`.
   * @param catalogName
   *   the name of the Iceberg catalog
   * @param catalogProperties
   *   any additional properties required by the Iceberg catalog; see:
   *   https://iceberg.apache.org/docs/latest/catalog-properties
   * @param hadoopConfigProperties
   *   any additional Hadoop configuration properties
   * @param triggeringFrequencySeconds
   *   (streaming only) frequency at which snapshots are produced
   * @param directWriteByteLimit
   *   (streaming only) limit for lifting bundles into the direct write path.
   *
   * For a complete reference, see:
   * https://docs.cloud.google.com/dataflow/docs/guides/managed-io-iceberg
   */
  def saveAsIceberg(
    table: String,
    catalogName: String = null,
    catalogProperties: Map[String, String] = IcebergIO.WriteParam.DefaultCatalogProperties,
    hadoopConfigProperties: Map[String, String] =
      IcebergIO.WriteParam.DefaultHadoopConfigProperties,
    triggeringFrequencySeconds: Int = IcebergIO.WriteParam.DefaultTriggeringFrequencySeconds,
    directWriteByteLimit: Int = IcebergIO.WriteParam.DefaultDirectWriteByteLimit
  ): ClosedTap[Nothing] = {

    val params = IcebergIO.WriteParam(
      catalogProperties,
      hadoopConfigProperties,
      Option(triggeringFrequencySeconds).filter(
        _ != IcebergIO.WriteParam.DefaultTriggeringFrequencySeconds
      ),
      Option(directWriteByteLimit).filter(_ != IcebergIO.WriteParam.DefaultDirectWriteByteLimit)
    )
    self.write(IcebergIO(table, Option(catalogName)))(params)
  }
}

trait SCollectionSyntax {
  implicit def icebergSCollectionSyntax[T: RowType: Coder](
    self: SCollection[T]
  ): IcebergSCollectionSyntax[T] =
    new IcebergSCollectionSyntax(self)
}
