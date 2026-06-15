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
import com.spotify.scio.values.SCollection
import magnolify.beam.RowType

class IcebergScioContextSyntax(self: ScioContext) {

  /**
   * Reads from an Iceberg table using Beam's [[org.apache.beam.sdk.managed.Managed]] IO.
   *
   * @param table
   *   the unique identifier for the Iceberg table, in the format `{table_namespace}.{table_name}`.
   * @param catalogName
   *   the name of the Iceberg catalog
   * @param catalogProperties
   *   any additional properties required by the Iceberg catalog; see:
   *   https://iceberg.apache.org/docs/latest/catalog-properties
   * @param configProperties
   *   any additional Hadoop configuration properties
   * @param filter
   *   the predicate to apply to the Iceberg read Multiple filters can be combined using AND/OR, for
   *   example: `__PARTITIONTIME > '2026-01-01' AND age > 25`
   *
   * For a complete reference, see:
   * https://docs.cloud.google.com/dataflow/docs/guides/managed-io-iceberg
   */
  def iceberg[T: Coder](
    table: String,
    catalogName: String = null,
    catalogProperties: Map[String, String] = IcebergIO.ReadParam.DefaultCatalogProperties,
    configProperties: Map[String, String] = IcebergIO.ReadParam.DefaultHadoopConfigProperties,
    filter: String = IcebergIO.ReadParam.DefaultFilter
  )(implicit rt: RowType[T]): SCollection[T] = {
    val params = IcebergIO.ReadParam(catalogProperties, configProperties, filter)
    self.read(IcebergIO(table, Option(catalogName)))(params)
  }
}

trait ScioContextSyntax {
  implicit def icebergScioContextSyntax(self: ScioContext): IcebergScioContextSyntax =
    new IcebergScioContextSyntax(self)
}
