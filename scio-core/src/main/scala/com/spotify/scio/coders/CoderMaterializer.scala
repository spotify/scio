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

package com.spotify.scio.coders

import org.apache.beam.sdk.coders.{Coder => BCoder, KvCoder, NullableCoder}
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory

object CoderMaterializer {
  import com.spotify.scio.ScioContext

  final def beam[T](sc: ScioContext, c: Coder[T]): BCoder[T] =
    beam(sc.pipeline.getCoderRegistry, sc.options, c)

  final def beamWithDefault[T](coder: Coder[T],
                               r: CoderRegistry = CoderRegistry.createDefault(),
                               o: PipelineOptions = PipelineOptionsFactory.create()): BCoder[T] =
    beam(r, o, coder)

  final def beam[T](r: CoderRegistry, o: PipelineOptions, coder: Coder[T]): BCoder[T] = {
    coder match {
      case Beam(c) =>
        val nullableCoder = o.as(classOf[com.spotify.scio.options.ScioOptions]).getNullableCoders()
        if (nullableCoder) NullableCoder.of(c)
        else c
      case Fallback(ct) =>
        WrappedBCoder.create(
          com.spotify.scio.Implicits
            .RichCoderRegistry(r)
            .getScalaCoder[T](o)(ct))
      case Transform(c, f) =>
        val u = f(beam(r, o, c))
        WrappedBCoder.create(beam(r, o, u))
      case Record(typeName, coders, construct, destruct) =>
        new RecordCoder(typeName, coders.map(c => c._1 -> beam(r, o, c._2)), construct, destruct)
      case Disjunction(typeName, idCoder, id, coders) =>
        WrappedBCoder.create(
          // `.map(identity) is really needed to make Map serializable.
          DisjunctionCoder(typeName,
                           beam(r, o, idCoder),
                           id,
                           coders.mapValues(u => beam(r, o, u)).map(identity)))
      case KVCoder(koder, voder) =>
        KvCoder.of(beam(r, o, koder), beam(r, o, voder))
    }
  }

  def kvCoder[K, V](ctx: ScioContext)(implicit k: Coder[K], v: Coder[V]): KvCoder[K, V] =
    KvCoder.of(beam(ctx, Coder[K]), beam(ctx, Coder[V]))
}
