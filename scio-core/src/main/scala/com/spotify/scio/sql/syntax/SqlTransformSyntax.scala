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

import com.spotify.scio.sql.{UdafFromCombineFn, Udf, UdfFromClass, UdfFromSerializableFn}
import org.apache.beam.sdk.extensions.sql.SqlTransform

import scala.language.implicitConversions

final class SqlTransformOps(private val t: SqlTransform) extends AnyVal {

  def registerUdf(udfs: Udf*): SqlTransform =
    udfs.foldLeft(t) {
      case (st, x: UdfFromClass[_]) =>
        st.registerUdf(x.fnName, x.clazz)
      case (st, x: UdfFromSerializableFn[_, _]) =>
        st.registerUdf(x.fnName, x.fn)
      case (st, x: UdafFromCombineFn[_, _, _]) =>
        st.registerUdaf(x.fnName, x.fn)
    }
}

trait SqlTransformSyntax {
  implicit def sqlSqlTransformOps(t: SqlTransform): SqlTransformOps = new SqlTransformOps(t)
}
