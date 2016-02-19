/*
 * Copyright 2016 Spotify AB.
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

import org.joda.time.Instant

object Schemas {

  // primitives
  case class P1(f1: Int, f2: Long, f3: Float, f4: Double, f5: Boolean, f6: String, f7: Instant)
  case class P2(f1: Option[Int], f2: Option[Long], f3: Option[Float], f4: Option[Double],
                f5: Option[Boolean], f6: Option[String], f7: Option[Instant])
  case class P3(f1: List[Int], f2: List[Long], f3: List[Float], f4: List[Double],
                f5: List[Boolean], f6: List[String], f7: List[Instant])

  // records
  case class R1(f1: P1, f2: P2, f3: P3)
  case class R2(f1: Option[P1], f2: Option[P2], f3: Option[P3])
  case class R3(f1: List[P1], f2: List[P2], f3: List[P3])

}
