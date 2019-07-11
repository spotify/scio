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

package com.spotify.scio.coders

import org.apache.beam.sdk.coders.{CoderRegistry, KvCoder, NullableCoder, Coder => BCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}

object CoderMaterializer {
  import com.spotify.scio.ScioContext

  final def beam[T](sc: ScioContext, c: Coder[T]): BCoder[T] =
    beam(sc.pipeline.getCoderRegistry, sc.options, c)

  final def beamWithDefault[T](
    coder: Coder[T],
    r: CoderRegistry = CoderRegistry.createDefault(),
    o: PipelineOptions = PipelineOptionsFactory.create()
  ): BCoder[T] = beam(r, o, coder)

  @inline private def nullCoder[T](o: PipelineOptions, c: BCoder[T]) = {
    val nullableCoder = o.as(classOf[com.spotify.scio.options.ScioOptions]).getNullableCoders()
    if (nullableCoder) NullableCoder.of(c)
    else c
  }

  final def beam[T](
    r: CoderRegistry,
    o: PipelineOptions,
    coder: Coder[T]
  ): BCoder[T] = beamImpl(r, o, coder, Map.empty)

  final private def beamImpl[T](
    r: CoderRegistry,
    o: PipelineOptions,
    coder: Coder[T],
    refs: Map[String, RefCoder[_]]
  ): BCoder[T] = {
    coder match {
      // #1734: do not wrap native beam coders
      case Beam(c) if c.getClass.getPackage.getName.startsWith("org.apache.beam") =>
        nullCoder(o, c)
      case Beam(c) =>
        WrappedBCoder.create(nullCoder(o, c))
      case Fallback(ct) =>
        val kryoCoder = new KryoAtomicCoder[T](KryoOptions(o))
        WrappedBCoder.create(nullCoder(o, kryoCoder))
      case Transform(c, f) =>
        val u = f(beamImpl(r, o, c, refs))
        WrappedBCoder.create(beamImpl(r, o, u, refs))
      case Record(typeName, coders, construct, destruct) =>
        WrappedBCoder.create(
          new RecordCoder(
            typeName,
            coders.map(c => c._1 -> nullCoder(o, beamImpl(r, o, c._2, refs))),
            construct,
            destruct
          )
        )
      case Disjunction(typeName, idCoder, id, coders) =>
        WrappedBCoder.create(
          // `.map(identity) is really needed to make Map serializable.
          DisjunctionCoder(
            typeName,
            beamImpl(r, o, idCoder, refs),
            id,
            coders.mapValues(u => beamImpl(r, o, u, refs)).map(identity)
          )
        )
      case KVCoder(koder, voder) =>
        WrappedBCoder.create(KvCoder.of(beamImpl(r, o, koder, refs), beamImpl(r, o, voder, refs)))
      case Ref(t, c) =>
        refs
          .get(t)
          .getOrElse {
            lazy val bc: BCoder[T] = beamImpl(r, o, c, refs + (t -> RefCoder(t, bc)))
            bc
          }
          .asInstanceOf[BCoder[T]]
    }
  }

  def kvCoder[K, V](ctx: ScioContext)(implicit k: Coder[K], v: Coder[V]): KvCoder[K, V] =
    KvCoder.of(beam(ctx, Coder[K]), beam(ctx, Coder[V]))
}
