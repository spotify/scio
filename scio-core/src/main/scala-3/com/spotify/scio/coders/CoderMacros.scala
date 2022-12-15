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

import com.spotify.scio.{FeatureFlag, MacroSettings} // {FeatureFlag, MacroSettings, MagnoliaMacros}
import com.spotify.scio.MagnoliaMacros
import scala.reflect.ClassTag

import scala.annotation.{nowarn, tailrec, experimental}
import scala.quoted._

object CoderMacros {
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

  @experimental
  @nowarn("msg=parameter value lp in method issueFallbackWarning is never used")
  def issueFallbackWarning[T: Type](using Quotes
    )(): Expr[Coder[T]] = { // TODO migration: shapeless LowPriority
    import quotes.reflect._

    print("issueFallBackWarning " + TypeRepr.of[T].show)
    val show = MacroSettings.showCoderFallback(summon[Quotes]) == FeatureFlag.Enable

    val wtt = TypeRepr.of[T]
    val sym = wtt.typeSymbol
    val args = wtt.typeArgs


    val typeName = sym.name
    val params = args.headOption
      .map(_ => args.mkString("[", ",", "]"))
      .getOrElse("")
    val fullType = s"$typeName$params"

    val toReport = Position.ofMacroExpansion.toString -> wtt.toString
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
        
    val fallback = '{ com.spotify.scio.coders.Coder.kryo[T](scala.compiletime.summonInline[ClassTag[T]]) }

    (verbose, alreadyReported) match {
      case _ if BlacklistedTypes.contains(wtt.toString) =>
        val msg =
          s"Can't use a Kryo coder for $wtt. You need to explicitly set the Coder for this type"
        report.errorAndAbort(msg)
      case _ if Warnings.contains(wtt.toString) =>
        report.info(Warnings(wtt.toString))
        fallback
      case (false, false) =>
        if (show) report.info(shortMessage.stripMargin) 
        fallback
      case (true, false) =>
        if (show) report.info(longMessage.stripMargin)
        verbose = false
        fallback
      case (_, _) =>
        fallback
    }
  }

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedCoder[T: Type](using Quotes): Expr[Coder[T]] = ??? //{ // TODO migration: not implemented yet
    // import quotes.reflect._

    // val wtt = TypeRepr.of[T]
    // // val imp = c.openImplicits match {
    // //   case Nil => None
    // //   case _   => companionImplicit(c)(wtt)
    // // }
    // val imp = companionImplicit(wtt)

    // // imp.map(_ => EmptyTree)
    // imp.map(_ => null.asInstanceOf[Expr[Coder[T]]]).getOrElse { // TODO null may not be correct
    //   // Magnolia does not support classes with a private constructor.
    //   // Workaround the limitation by using a fallback in that case
    //   privateConstructor(wtt).fold(MagnoliaMacros.genWithoutAnnotations[T]) { _ =>
    //     '{ com.spotify.scio.coders.Coder.fallback[T]() }
    //   }.asExprOf[Coder[T]]
    // }
  // }

  private[this] def companionImplicit(using Quotes)(tpe: quotes.reflect.TypeRepr): Option[quotes.reflect.Symbol] = {
    import quotes.reflect._
    val tp = tpe.asType match
      case '[t] => TypeRepr.of[com.spotify.scio.coders.Coder[t]]
    
    def resultType(typeRepr: TypeRepr): TypeRepr = typeRepr match
      case MethodType(_, _, retType) => retType
      case retType => retType
    
    val companion = tpe.typeSymbol.companionModule
    (companion.methodMembers ++ companion.fieldMembers).iterator.filter(_.flags.is(Flags.Implicit)).find(i => resultType(companion.memberType(i.name).typeRef) =:= tp)
  }

  private[this] def privateConstructor(using Quotes)(tpe: quotes.reflect.TypeRepr): Option[quotes.reflect.Symbol] =
    import quotes.reflect._
    tpe.typeSymbol.declarations.find(m => m.isClassConstructor && m.flags.is(Flags.Private))
}
