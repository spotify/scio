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
package com.spotify.scio.spanner.instances

import com.google.cloud.spanner.{Mutation, Struct}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.{coders => bcoders}
import org.apache.beam.sdk.io.gcp.spanner.{MutationGroup, ReadOperation}

trait CoderInstances {
  // Kryo coders throw runtime exceptions deserializing Spanner types (#1447), so force Beam coders
  implicit val spannerReadOperationCoder: Coder[ReadOperation] =
    Coder.beam(bcoders.SerializableCoder.of(classOf[ReadOperation]))

  implicit val spannerStructCoder: Coder[Struct] =
    Coder.beam(bcoders.SerializableCoder.of(classOf[Struct]))

  implicit val spannerMutationCoder: Coder[Mutation] =
    Coder.beam(bcoders.SerializableCoder.of(classOf[Mutation]))

  implicit val spannerMutationGroupCoder: Coder[MutationGroup] =
    Coder.beam(bcoders.SerializableCoder.of(classOf[MutationGroup]))
}

//scalastyle:off object.name
object coders extends CoderInstances
//scalastyle:on object.name
