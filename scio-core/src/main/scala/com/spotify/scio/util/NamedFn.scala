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

package com.spotify.scio.util

import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.transforms.{
  DoFn,
  ProcessFunction,
  SerializableBiFunction,
  SerializableFunction,
  SimpleFunction
}

/** Helper trait to decorate anonymous functions with a meaningful toString. */
private[util] trait NamedFn {
  private val callSite: String = CallSites.getCurrent
  override def toString: String = s"anonymous function $callSite"
}

private[util] trait NamedProcessFn[T, U] extends ProcessFunction[T, U] with NamedFn
private[util] trait NamedSerializableFn[T, U] extends SerializableFunction[T, U] with NamedFn
private[util] trait NamedSerializableBiFn[T, G, U]
    extends SerializableBiFunction[T, G, U]
    with NamedFn
private[util] trait NamedPartitionFn[T] extends PartitionFn[T] with NamedFn
private[scio] class NamedDoFn[T, U] extends DoFn[T, U] with NamedFn
private[util] class NamedSimpleFn[T, U] extends SimpleFunction[T, U] with NamedFn
