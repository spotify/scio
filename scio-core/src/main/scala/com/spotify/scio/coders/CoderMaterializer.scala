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

import scala.collection.concurrent.TrieMap
import scala.util.chaining._

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

  final def beam[T](
    o: PipelineOptions,
    coder: Coder[T]
  ): BCoder[T] = beamImpl(CoderOptions(o), coder, refs = TrieMap.empty, topLevel = true)

  private def isNullableCoder(o: CoderOptions, c: Coder[_]): Boolean = c match {
    case _: RawBeam[_]      => false // raw cannot be made nullable
    case _: KVCoder[_, _]   => false // KV cannot be made nullable
    case _: Transform[_, _] => false // nullability should be deferred to transformed coders
    case _: Ref[_]          => false // nullability should be deferred to underlying coder
    case _                  => o.nullableCoders
  }

  private def isWrappableCoder(topLevel: Boolean, c: Coder[_]): Boolean = c match {
    case _: RawBeam[_]    => false // raw should not be wrapped
    case _: KVCoder[_, _] => false // KV should not be wrapped, but independent k,v can
    case _                => topLevel
  }

  final private[scio] def beamImpl[T](
    o: CoderOptions,
    coder: Coder[T],
    refs: TrieMap[Ref[_], RefCoder[_]],
    topLevel: Boolean = false
  ): BCoder[T] = {
    val bCoder: BCoder[T] = coder match {
      case RawBeam(c) =>
        c
      case Beam(c) =>
        c
      case Fallback(_) =>
        new KryoAtomicCoder[T](o.kryo)
      case Transform(c, f) =>
        val uc = f(beamImpl(o, c, refs))
        beamImpl(o, uc, refs)
      case Record(typeName, coders, construct, destruct) =>
        RecordCoder(
          typeName,
          coders.map { case (n, c) => n -> beamImpl(o, c, refs) },
          construct,
          destruct
        )
      case Disjunction(typeName, idCoder, id, coders) =>
        DisjunctionCoder(
          typeName,
          beamImpl(o, idCoder, refs),
          id,
          coders.map { case (k, u) => k -> beamImpl(o, u, refs) }
        )
      case KVCoder(koder, voder) =>
        // propagate topLevel to k & v coders
        val kbc = beamImpl(o, koder, refs, topLevel)
        val vbc = beamImpl(o, voder, refs, topLevel)
        KvCoder.of(kbc, vbc)
      case r @ Ref(t, c) =>
        refs.get(r) match {
          case Some(rc) =>
            LazyCoder(t, rc.bcoder.asInstanceOf[BCoder[T]])
          case None =>
            val rc = RefCoder[T]()
            refs += r -> rc
            rc.bcoder = beamImpl(o, c, refs)
            rc
        }
    }

    bCoder
      .pipe(bc => if (isNullableCoder(o, coder)) NullableCoder.of(bc) else bc)
      .pipe(bc => if (isWrappableCoder(topLevel, coder)) WrappedBCoder(bc) else bc)
  }
}
