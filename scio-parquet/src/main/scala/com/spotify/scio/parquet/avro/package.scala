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

package com.spotify.scio.parquet

import com.spotify.scio.parquet.avro.syntax.Syntax
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, GenericDataSupplier}

/**
 * Main package for Parquet Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.avro._
 * }}}
 */
package object avro extends Syntax {

  /** Alias for `me.lyh.parquet.avro.Projection`. */
  val Projection = me.lyh.parquet.avro.Projection

  /** Alias for `me.lyh.parquet.avro.Predicate`. */
  val Predicate = me.lyh.parquet.avro.Predicate

  implicit class ConfigurationSyntax(conf: Configuration) {

    def setReadSchemas[A, T](params: ParquetAvroIO.ReadParam[A, T]): Unit = {
      AvroReadSupport.setAvroReadSchema(conf, params.readSchema)
      if (params.projection != null) {
        AvroReadSupport.setRequestedProjection(conf, params.projection)
      }
    }

    def setGenericDataSupplierByDefault(): Unit = {
      if (conf.get(AvroReadSupport.AVRO_DATA_SUPPLIER) == null) {
        conf.setClass(
          AvroReadSupport.AVRO_DATA_SUPPLIER,
          classOf[GenericDataSupplier],
          classOf[AvroDataSupplier]
        )
      }
    }

  }

}
