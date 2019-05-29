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
package com.spotify.scio.sql.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.schemas.Schema
import com.spotify.scio.sql.{Sql, SqlSCollection1}

import scala.language.implicitConversions

trait SCollectionSyntax {
  implicit def sqlSCollectionOps[A: Schema](sc: SCollection[A]): SqlSCollection1[A] = Sql.from(sc)
}
