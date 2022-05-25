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

import org.apache.beam.sdk.coders.{Coder => BCoder, KvCoder, NullableCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}

object CoderMaterializer {
  import com.spotify.scio.ScioContext

  private[scio] case class CoderOptions(nullableCoders: Boolean, kryo: KryoOptions)
  private[scio] object CoderOptions {
    final def apply(o: PipelineOptions): CoderOptions = {
      val nullableCoder = o.as(classOf[com.spotify.scio.options.ScioOptions]).getNullableCoders
      new CoderOptions(nullableCoder, KryoOptions(o))
    }
  }

  final def beam[T](sc: ScioContext, c: Coder[T]): BCoder[T] =
    beam(sc.options, c)

  final def beamWithDefault[T](
    coder: Coder[T],
    o: PipelineOptions = PipelineOptionsFactory.create()
  ): BCoder[T] = beam(o, coder)

  @inline private def nullCoder[T](o: CoderOptions, c: BCoder[T]) =
    if (o.nullableCoders) NullableCoder.of(c)
    else c

  final def beam[T](
    o: PipelineOptions,
    coder: Coder[T]
  ): BCoder[T] = beamImpl(CoderOptions(o), coder, materializeStack = true)

  final private[scio] def beamImpl[T](
    o: CoderOptions,
    coder: Coder[T],
    materializeStack: Boolean = false
  ): BCoder[T] =
    coder match {
      case RawBeam(c) => c
      // #1734: do not wrap native beam coders
      case Beam(c) if c.getClass.getPackage.getName.startsWith("org.apache.beam") =>
        nullCoder(o, c)
      case Beam(c) =>
        WrappedBCoder(nullCoder(o, c), materializeStack)
      case Fallback(_) =>
        val kryoCoder = new KryoAtomicCoder[T](o.kryo)
        WrappedBCoder(nullCoder(o, kryoCoder), materializeStack)
      case Transform(c, f) =>
        val u = f(beamImpl(o, c))
        WrappedBCoder(beamImpl(o, u), materializeStack)
      case Record(typeName, coders, construct, destruct) =>
        val recordCoder = RecordCoder(
          typeName,
          coders.map(c => c._1 -> nullCoder(o, beamImpl(o, c._2))),
          construct,
          destruct
        )
        WrappedBCoder(nullCoder(o, recordCoder), materializeStack)
      case Disjunction(typeName, idCoder, id, coders) =>
        val disjunctionCoder = DisjunctionCoder(
          typeName,
          beamImpl(o, idCoder),
          id,
          coders.iterator.map { case (k, u) => (k, beamImpl(o, u)) }.toMap
        )
        WrappedBCoder(nullCoder(o, disjunctionCoder), materializeStack)
      case KVCoder(koder, voder) =>
        val kvCoder = KvCoder.of(beamImpl(o, koder), beamImpl(o, voder))
        WrappedBCoder(kvCoder, materializeStack)
      case Ref(t, c) =>
        val lazyCoder = LazyCoder[T](t, o)(c)
        WrappedBCoder(lazyCoder, materializeStack)
    }

  def kvCoder[K, V](ctx: ScioContext)(implicit k: Coder[K], v: Coder[V]): KvCoder[K, V] =
    KvCoder.of(beam(ctx, k), beam(ctx, v))
}
