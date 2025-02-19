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

package com.spotify.scio.bigquery

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.spotify.scio.coders.Coder
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes
import org.typelevel.scalaccompat.annotation.nowarn

import java.math.MathContext
import java.nio.ByteBuffer
import scala.annotation.StaticAnnotation
package object types {

  /**
   * Case class field annotation for BigQuery field description.
   *
   * To be used with case class fields annotated with [[BigQueryType.toTable]], For example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class User(@description("user name") name: String,
   *                 @description("user age") age: Int)
   * }}}
   */
  @nowarn
  final class description(value: String) extends StaticAnnotation with Serializable

  /**
   * Case class to serve as raw type for Geography instances to distinguish them from Strings.
   *
   * See also
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#geography_type
   *
   * @param wkt
   *   Well Known Text formatted string that BigQuery displays for Geography
   */
  case class Geography(wkt: String)

  /**
   * Case class to serve as raw type for Json instances.
   *
   * See also https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#json_type
   *
   * @param wkt
   *   Well Known Text formatted string that BigQuery displays for Json
   */
  case class Json(wkt: String)
  object Json {
    // Use same mapper as the TableRowJsonCoder
    private lazy val mapper = new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .registerModule(new JodaModule())
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    def parse(json: Json): AnyRef = mapper.readValue(json.wkt, classOf[Object])
  }

  /**
   * Case class to serve as BigNumeric type to distinguish them from Numeric.
   *
   * See also https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
   *
   * @param wkt
   *   Well Known big numeric
   */
  case class BigNumeric private (wkt: BigDecimal)
  object BigNumeric {
    implicit val bigNumericCoder: Coder[BigNumeric] =
      Coder.xmap(Coder[BigDecimal])(new BigNumeric(_), _.wkt)

    val MaxNumericPrecision = 77
    val MaxNumericScale = 38

    private val DecimalConverter = new DecimalConversion
    private val DecimalLogicalType = LogicalTypes.decimal(MaxNumericPrecision, MaxNumericScale)

    def apply(value: String): BigNumeric = apply(BigDecimal(value))

    def apply(value: BigDecimal): BigNumeric = {
      val scaled = if (value.scale > MaxNumericScale) {
        value.setScale(MaxNumericScale, scala.math.BigDecimal.RoundingMode.HALF_UP)
      } else {
        value
      }
      require(
        scaled.precision <= MaxNumericPrecision,
        s"max allowed precision is $MaxNumericPrecision"
      )

      val wkt = scaled.round(new MathContext(MaxNumericPrecision))
      new BigNumeric(wkt)
    }

    // For BigQueryType macros only, do not use directly
    def parse(value: Any): BigNumeric = value match {
      case b: ByteBuffer => new BigNumeric(DecimalConverter.fromBytes(b, null, DecimalLogicalType))
      case _             => apply(value.toString)
    }

    def bytes(value: BigNumeric): ByteBuffer =
      DecimalConverter.toBytes(value.wkt.bigDecimal, null, DecimalLogicalType)
  }
}
