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

import com.spotify.scio.util.RemoteFileUtil
import org.apache.beam.sdk.coders.{
  Coder => BCoder,
  IterableCoder,
  KvCoder,
  NullableCoder,
  ZstdCoder
}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.commons.lang3.ObjectUtils

import java.net.URI
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat._
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.chaining._

object CoderMaterializer {
  import com.spotify.scio.ScioContext

  private[scio] case class CoderOptions(
    nullableCoders: Boolean,
    kryo: KryoOptions,
    zstdDictMapping: Map[String, Array[Byte]]
  )
  private[scio] object CoderOptions {
    private val cache: ConcurrentHashMap[String, CoderOptions] = new ConcurrentHashMap()
    private val ZstdArgRegex = "([^:]+):(.*)".r
    private val ZstdPackageBlacklist =
      List("scala.", "java.", "com.spotify.scio.", "org.apache.beam.")

    final def apply(o: PipelineOptions): CoderOptions = {
      cache.computeIfAbsent(
        ObjectUtils.identityToString(o),
        { _ =>
          val scioOpts = o.as(classOf[com.spotify.scio.options.ScioOptions])
          val nullableCoder = scioOpts.getNullableCoders

          val (errors, classPathMapping) = Option(scioOpts.getZstdDictionary)
            .map(_.asScala.toList)
            .getOrElse(List.empty)
            .partitionMap {
              case s @ ZstdArgRegex(className, path) =>
                Option
                  .when(ZstdPackageBlacklist.exists(className.startsWith))(
                    s"zstdDictionary command-line arguments may not be used for class $className. " +
                      s"Provide Zstd coders manually instead."
                  )
                  .orElse {
                    Try(Class.forName(className)).failed.toOption
                      .map(_ => s"Class for zstdDictionary argument ${s} not found.")
                  }
                  .toLeft(className.replaceAll("\\$", ".") -> path)
              case s =>
                Left(
                  "zstdDictionary arguments must be in a colon-separated format. " +
                    s"Example: `com.spotify.ClassName:gs://path`. Found: $s"
                )
            }

          if (errors.nonEmpty) {
            throw new IllegalArgumentException(
              errors.mkString("Bad zstdDictionary arguments:\n\t", "\n\t", "\n")
            )
          }

          val zstdDictPaths = classPathMapping
            .groupBy(_._1)
            .map { case (className, values) => className -> values.map(_._2).toSet }

          val dupes = zstdDictPaths
            .collect {
              case (className, values) if values.size > 1 =>
                s"Class $className -> [${values.mkString(", ")}]"
            }
          if (dupes.size > 1) {
            throw new IllegalArgumentException(
              dupes.mkString("Found multiple Zstd dictionaries for:\n\t", "\n\t", "\n")
            )
          }

          val zstdDictMapping = zstdDictPaths.map { case (clazz, dictUriSet) =>
            // dictUriSet always contains exactly 1 item
            val dictUri = dictUriSet.toList.head
            val dictPath = RemoteFileUtil.create(o).download(new URI(dictUri))
            val dictBytes = Files.readAllBytes(dictPath)
            Files.delete(dictPath)
            clazz -> dictBytes
          }

          new CoderOptions(nullableCoder, KryoOptions(o), zstdDictMapping)
        }
      )
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
    case Beam(_: NullableCoder[_]) => false // already nullable
    case _: RawBeam[_]             => false // raw cannot be made nullable
    case _: KVCoder[_, _]          => false // KV cannot be made nullable
    case _: AggregateCoder[_]      => false // aggregate cannot be made nullable
    case _: Transform[_, _]        => false // nullability should be deferred to transformed coders
    case _: Ref[_]                 => false // nullability should be deferred to underlying coder
    case _                         => o.nullableCoders
  }

  private def isWrappableCoder(topLevel: Boolean, c: Coder[_]): Boolean = c match {
    case _: RawBeam[_]        => false // raw should not be wrapped
    case _: KVCoder[_, _]     => false // KV should not be wrapped, but independent k,v can
    case _: AggregateCoder[_] => false // aggregate should not be wrapped, but value can
    case _                    => topLevel
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
      case CoderTransform(_, coder, from) =>
        val underlying = beamImpl(o, coder, refs)
        beamImpl(o, from(underlying), refs)
      case Transform(typeName, c, t, f) =>
        new TransformCoder(typeName, beamImpl(o, c, refs), t, f)
      case Singleton(typeName, supply) =>
        new SingletonCoder(typeName, supply)
      case Record(typeName, coders, construct, destruct) =>
        new RecordCoder(
          typeName,
          coders.map { case (n, c) => n -> beamImpl(o, c, refs) },
          construct,
          destruct
        )
      case Disjunction(typeName, idCoder, coders, id) =>
        new DisjunctionCoder(
          typeName,
          beamImpl(o, idCoder, refs),
          coders.map { case (k, u) => k -> beamImpl(o, u, refs) },
          id
        )
      case AggregateCoder(c) =>
        IterableCoder.of(beamImpl(o, c, refs, topLevel))
      case KVCoder(koder, voder) =>
        // propagate topLevel to k & v coders
        val kbc = beamImpl(o, koder, refs, topLevel)
        val vbc = beamImpl(o, voder, refs, topLevel)
        KvCoder.of(kbc, vbc)
      case r @ Ref(t, c) =>
        refs.get(r) match {
          case Some(rc) =>
            new LazyCoder(t, rc.bcoder.asInstanceOf[BCoder[T]])
          case None =>
            val rc = new RefCoder[T]()
            refs += r -> rc
            rc.bcoder = beamImpl(o, c, refs)
            rc
        }
    }

    bCoder
      .pipe(bc => if (isNullableCoder(o, coder)) NullableCoder.of(bc) else bc)
      .pipe { bc =>
        Option(coder)
          .collect { case x: TypeName => x.typeName }
          .flatMap(o.zstdDictMapping.get)
          .map(ZstdCoder.of(bc, _))
          .getOrElse(bc)
      }
      .pipe(bc => if (isWrappableCoder(topLevel, coder)) new MaterializedCoder(bc) else bc)
  }
}
