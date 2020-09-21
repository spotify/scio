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

package com.spotify.scio.bigquery.types

import com.google.protobuf.ByteString
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

object Schemas {
  // primitives
  case class Required(
    boolF: Boolean,
    intF: Int,
    longF: Long,
    floatF: Float,
    doubleF: Double,
    stringF: String,
    byteArrayF: Array[Byte],
    byteStringF: ByteString,
    timestampF: Instant,
    dateF: LocalDate,
    timeF: LocalTime,
    datetimeF: LocalDateTime,
    bigDecimalF: BigDecimal,
    geographyF: Geography
  )
  case class Optional(
    boolF: Option[Boolean],
    intF: Option[Int],
    longF: Option[Long],
    floatF: Option[Float],
    doubleF: Option[Double],
    stringF: Option[String],
    byteArrayF: Option[Array[Byte]],
    byteStringF: Option[ByteString],
    timestampF: Option[Instant],
    dateF: Option[LocalDate],
    timeF: Option[LocalTime],
    datetimeF: Option[LocalDateTime],
    bigDecimalF: Option[BigDecimal],
    geographyF: Option[Geography]
  )
  case class Repeated(
    boolF: List[Boolean],
    intF: List[Int],
    longF: List[Long],
    floatF: List[Float],
    doubleF: List[Double],
    stringF: List[String],
    byteArrayF: List[Array[Byte]],
    byteStringF: List[ByteString],
    timestampF: List[Instant],
    dateF: List[LocalDate],
    timeF: List[LocalTime],
    datetimeF: List[LocalDateTime],
    bigDecimalF: List[BigDecimal],
    geographyF: List[Geography]
  )

  // records
  case class RequiredNested(required: Required, optional: Optional, repeated: Repeated)
  case class OptionalNested(
    required: Option[Required],
    optional: Option[Optional],
    repeated: Option[Repeated]
  )
  case class RepeatedNested(
    required: List[Required],
    optional: List[Optional],
    repeated: List[Repeated]
  )

  case class User(@description("user name") name: String, @description("user age") age: Int)
  case class Account(
    @description("account user") user: User,
    @description("in USD") balance: Double
  )
}
