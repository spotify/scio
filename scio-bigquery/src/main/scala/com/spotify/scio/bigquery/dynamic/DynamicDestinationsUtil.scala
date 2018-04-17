/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.bigquery.dynamic

import com.google.api.services.bigquery.model.TableSchema
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.gcp.bigquery.{
  DynamicDestinations,
  TableDestination,
  TableDestinationCoder
}
import org.apache.beam.sdk.util.Transport
import org.apache.beam.sdk.values.ValueInSingleWindow

private[dynamic] object DynamicDestinationsUtil {

  def constant[T](dst: TableDestination,
                  schema: TableSchema): DynamicDestinations[T, TableDestination] =
    tableFn(_ => dst, schema)

  def tableFn[T](fn: ValueInSingleWindow[T] => TableDestination,
                 schema: TableSchema): DynamicDestinations[T, TableDestination] = {
    val jsonSchema = Transport.getJsonFactory.toString(schema)
    new DynamicDestinations[T, TableDestination] {
      @transient private lazy val tableSchema =
        Transport.getJsonFactory.fromString(jsonSchema, classOf[TableSchema])

      override def getDestination(element: ValueInSingleWindow[T]): TableDestination =
        fn(element)

      override def getSchema(destination: TableDestination): TableSchema = tableSchema

      override def getTable(destination: TableDestination): TableDestination = destination

      override def getDestinationCoder: Coder[TableDestination] = TableDestinationCoder.of
    }
  }

}
