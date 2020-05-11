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

import com.spotify.scio.{FeatureFlag, MacroSettings, MagnoliaMacros}

import scala.reflect.macros._

private[coders] object CoderMacros {
  private[this] var verbose = true
  private[this] val reported: scala.collection.mutable.Set[(String, String)] =
    scala.collection.mutable.Set.empty

  private[this] val BlacklistedTypes = List("org.apache.beam.sdk.values.Row")

  private[this] val Warnings =
    Map(
      "org.apache.avro.generic.GenericRecord" ->
        """
          |Using a fallback coder for Avro's GenericRecord is discouraged as it is VERY inefficient.
          |It is highly recommended to define a proper Coder[GenericRecord] using:
          |
          |  Coder.avroGenericRecordCoder(schema)
        """.stripMargin
    )

  def issueFallbackWarning[T: c.WeakTypeTag](
    c: whitebox.Context
  )(lp: c.Expr[shapeless.LowPriority]): c.Tree = {
    import c.universe._

    val show = MacroSettings.showCoderFallback(c) == FeatureFlag.Enable

    val wtt = weakTypeOf[T]
    val TypeRef(_, sym, args) = wtt

    val typeName = sym.name
    val params = args.headOption
      .map(_ => args.mkString("[", ",", "]"))
      .getOrElse("")
    val fullType = typeName + params

    val toReport = c.enclosingPosition.toString -> wtt.toString
    val alreadyReported = reported.contains(toReport)
    if (!alreadyReported) reported += toReport

    val shortMessage =
      s"""
      | Warning: No implicit Coder found for the following type:
      |
      |   >> $wtt
      |
      | using Kryo fallback instead.
      """

    val longMessage =
      shortMessage +
        s"""
        |
        |  Scio will use a fallback Kryo coder instead.
        |
        |  If a type is not supported, consider implementing your own implicit Coder for this type.
        |  It is recommended to declare this Coder in your class companion object:
        |
        |       object $typeName {
        |         import com.spotify.scio.coders.Coder
        |         import org.apache.beam.sdk.coders.AtomicCoder
        |
        |         implicit def coder$typeName: Coder[$fullType] =
        |           Coder.beam(new AtomicCoder[$fullType] {
        |             def decode(in: InputStream): $fullType = ???
        |             def encode(ts: $fullType, out: OutputStream): Unit = ???
        |           })
        |       }
        |
        |  If you do want to use a Kryo coder, be explicit about it:
        |
        |       implicit def coder$typeName: Coder[$fullType] = Coder.kryo[$fullType]
        |
        |  Additional info at:
        |   - https://spotify.github.io/scio/internals/Coders
        |
        """

    val fallback = q"""_root_.com.spotify.scio.coders.Coder.kryo[$wtt]"""

    (verbose, alreadyReported) match {
      case _ if BlacklistedTypes.contains(wtt.toString) =>
        val msg =
          s"Can't use a Kryo coder for $wtt. You need to explicitly set the Coder for this type"
        c.abort(c.enclosingPosition, msg)
      case _ if Warnings.contains(wtt.toString) =>
        c.echo(c.enclosingPosition, Warnings(wtt.toString))
        fallback
      case (false, false) =>
        if (show) c.echo(c.enclosingPosition, shortMessage.stripMargin)
        fallback
      case (true, false) =>
        if (show) c.echo(c.enclosingPosition, longMessage.stripMargin)
        verbose = false
        fallback
      case (_, _) =>
        fallback
    }
  }

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedCoder[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val imp = c.openImplicits match {
      case Nil => None
      case _   => companionImplicit(c)(wtt)
    }

    imp.map(_ => EmptyTree).getOrElse {
      // Magnolia does not support classes with a private constructor.
      // Workaround the limitation by using a fallback in that case
      privateConstructor(c)(wtt).fold(MagnoliaMacros.genWithoutAnnotations[T](c)) { _ =>
        q"_root_.com.spotify.scio.coders.Coder.fallback[$wtt](null)"
      }
    }
  }

  private[this] def companionImplicit(c: whitebox.Context)(tpe: c.Type): Option[c.Symbol] = {
    import c.universe._
    val tp = c.typecheck(tq"_root_.com.spotify.scio.coders.Coder[$tpe]", c.TYPEmode).tpe
    tpe.companion.members.iterator.filter(_.isImplicit).find(_.info.resultType =:= tp)
  }

  private[this] def privateConstructor(c: whitebox.Context)(tpe: c.Type): Option[c.Symbol] =
    tpe.decls.find(m => m.isConstructor && m.isPrivate)
}
