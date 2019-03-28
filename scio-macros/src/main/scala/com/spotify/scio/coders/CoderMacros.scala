/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio.MagnoliaMacros

import scala.reflect.macros._

private[coders] object CoderMacros {

  private[this] var verbose = true
  private[this] val reported: scala.collection.mutable.Set[(String, String)] =
    scala.collection.mutable.Set.empty

  private[this] val ShowWarnDefault = false
  private[this] val ShowWarnSettingRegex = "show-coder-fallback=(true|false)".r

  private[this] val BlacklistedTypes = List("org.apache.beam.sdk.values.Row")

  private[this] val Warnings =
    Map(
      "org.apache.avro.generic.GenericRecord" ->
        """
          |Using a fallback coder for Avro's GenericRecord is discouraged as it is VERY ineficient.
          |It is highly recommended to define a proper Coder[GenericRecord] using:
          |
          |  Coder.avroGenericRecordCoder(schema)
        """.stripMargin
    )

  /**
   * Makes it possible to configure fallback warnings by passing
   * "-Xmacro-settings:show-coder-fallback=true" as a Scalac option.
   */
  private[this] def showWarn(c: whitebox.Context) =
    c.settings
      .collectFirst {
        case ShowWarnSettingRegex(value) =>
          value.toBoolean
      }
      .getOrElse(ShowWarnDefault)

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  def issueFallbackWarning[T: c.WeakTypeTag](c: whitebox.Context)(
    lp: c.Expr[shapeless.LowPriority]): c.Tree = {
    import c.universe._

    val show = showWarn(c)

    val wtt = weakTypeOf[T]
    val TypeRef(_, sym, args) = wtt

    val typeName = sym.name
    val params = args.headOption
      .map { _ =>
        args.mkString("[", ",", "]")
      }
      .getOrElse("")
    val fullType = typeName + params

    val toReport = c.enclosingPosition.toString -> wtt.toString
    val alreadyReported = reported.contains(toReport)
    if (!alreadyReported) reported += toReport

    def shortMessage =
      s"""
      | Warning: No implicit Coder found for the following type:
      |
      |   >> $wtt
      |
      | using Kryo fallback instead.
      """

    def longMessage =
      shortMessage +
        s"""
        |
        |  Scio will use a fallback Kryo coder instead.
        |
        |  If a type is not supported, consider implementing your own implicit Coder for this type.
        |  It is recommended to declare this Coder in your class companion object:
        |
        |       object ${typeName} {
        |         import com.spotify.scio.coders.Coder
        |         import org.apache.beam.sdk.coders.AtomicCoder
        |
        |         implicit def coder${typeName}: Coder[$fullType] =
        |           Coder.beam(new AtomicCoder[$fullType] {
        |             def decode(in: InputStream): $fullType = ???
        |             def encode(ts: $fullType, out: OutputStream): Unit = ???
        |           })
        |       }
        |
        |  If you do want to use a Kryo coder, be explicit about it:
        |
        |       implicit def coder${typeName}: Coder[$fullType] = Coder.kryo[$fullType]
        |
        |  Additional info at:
        |   - https://spotify.github.io/scio/internals/Coders
        |
        """

    val fallback = q"""_root_.com.spotify.scio.coders.Coder.kryo[$wtt]"""

    (verbose, alreadyReported) match {
      case _ if BlacklistedTypes.contains(wtt.toString) =>
        val msg =
          s"Can't use a Kryo coder for ${wtt}. You need to explicitly set the Coder for this type"
        c.abort(c.enclosingPosition, msg)
      case _ if Warnings.get(wtt.toString).isDefined =>
        Warnings.get(wtt.toString).foreach { m =>
          c.echo(c.enclosingPosition, m)
        }
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
  // scalastyle:on cyclomatic.complexity

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedCoder[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]

    if (wtt <:< typeOf[Iterable[_]]) {
      c.abort(c.enclosingPosition,
              s"Automatic coder derivation can't derive a Coder for $wtt <: Seq")
    }

    val magTree = MagnoliaMacros.genWithoutAnnotations[T](c)

    val isPrivateContructor =
      wtt.decls
        .collectFirst {
          case m: MethodSymbol if m.isConstructor =>
            m.isPrivate
        }
        .getOrElse(false)

    val tree: c.Tree =
      if (isPrivateContructor) {
        // Magnolia does not support classes with a private constructor.
        // Workaround the limitation by using a fallback in that case
        q"""_root_.com.spotify.scio.coders.Coder.fallback[$wtt](null)"""
      } else {
        //XXX: find a way to get rid of $outer references at compile time
        magTree
      }

    tree
  }
  // scalastyle:on method.length

}
