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
package com.spotify.scio.sql

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.SerializableFunction

sealed trait Udf { val fnName: String }

private final case class UdfFromSerializableFn[I, O](
  fnName: String,
  fn: SerializableFunction[I, O]
) extends Udf

private final case class UdfFromClass[T <: BeamSqlUdf](
  fnName: String,
  clazz: Class[T]
) extends Udf

private final case class UdafFromCombineFn[I1, I2, O](
  fnName: String,
  fn: CombineFn[I1, I2, O]
) extends Udf

object Udf {
  def fromSerializableFn[I, O](fnName: String, fn: SerializableFunction[I, O]): Udf =
    UdfFromSerializableFn(fnName, fn)

  def fromClass[T <: BeamSqlUdf](fnName: String, clazz: Class[T]): Udf =
    UdfFromClass(fnName, clazz)

  // @todo: should UDAFs also extend the Udf trait?
  def fromAggregateFn[I1, I2, O](fnName: String, fn: CombineFn[I1, I2, O]): Udf =
    UdafFromCombineFn(fnName, fn)
}
