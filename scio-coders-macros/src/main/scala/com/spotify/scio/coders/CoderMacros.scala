/*
 * Copyright 2017 Spotify AB.
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

import org.apache.avro.specific.SpecificRecordBase
import scala.reflect.macros._

private[coders] object CoderMacros {


  /**
  * Generate a coder which does not serialize the schema and relies exclusively on types.
  */
  def staticInvokeCoder[T <: SpecificRecordBase : c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val companionSymbol = companioned.companion
    val companionType = companionSymbol.typeSignature

    q"""
    _root_.com.spotify.scio.coders.Coder.beam(
      _root_.com.spotify.scio.coders.AvroRawCoder[$companioned](
        ${companionType}.getClassSchema()
      )
    )
    """
  }

  var verbose = true
  val reported: scala.collection.mutable.Set[(String, String)] =
    scala.collection.mutable.Set.empty

  // scalastyle:off method.length
  def issueFallbackWarning[T: c.WeakTypeTag](
    c: whitebox.Context)(lp: c.Expr[shapeless.LowPriority]): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val TypeRef(pre, sym, args) = wtt
    val companioned = wtt.typeSymbol

    val typeName = sym.name
    val params = args.headOption.map { _ => args.mkString("[", ",", "]") }.getOrElse("")
    val fullType = typeName + params

    val toReport = (c.enclosingPosition.toString -> wtt.toString)
    val alreadyReported = reported.contains(toReport)
    if(!alreadyReported) reported += toReport

    def shortMessage =
      s"""
      | Warning: No implicit Coder found for type, using Kryo fallback:
      |
      |   >> $wtt
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
        """

    val fallback = q"""_root_.com.spotify.scio.coders.Coder.kryo[$wtt]"""

    (verbose, alreadyReported) match {
      case (false, false) =>
        c.echo(c.enclosingPosition, shortMessage.stripMargin)
        fallback
      case (true, false) =>
        c.echo(c.enclosingPosition, longMessage.stripMargin)
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
    val companioned = wtt.typeSymbol

    if(wtt <:< typeOf[Iterable[_]]) {
      c.abort(c.enclosingPosition,
        s"Automatic coder derivation can't derive a Coder for $wtt <: Seq")
    }

    val magTree = magnolia.Magnolia.gen[T](c)

    def getLazyVal =
      magTree match {
        case q"val $name = $body; $rest" =>
          body
      }

    val name = c.freshName(s"$$ShimDerivedCoder")
    val className = TypeName(name)
    val termName = TermName(name)

    // Remove annotations from magnolia since they are
    // not serialiazable and we don't use them anyway
    // scalastyle:off line.size.limit
    val removeAnnotations =
      new Transformer {
        override def transform(tree: Tree) = {
          tree match {
            case Apply(AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps), List(typeName, isObject, isValueClass, params, annotations)) =>
              Apply(AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps), List(typeName, isObject, isValueClass, params, q"""Array()"""))
            case q"""new magnolia.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, $annotations){ $body }""" =>
              q"""_root_.magnolia.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, Array()){ $body }"""
            case q"com.spotify.scio.coders.Coder.dispatch(new magnolia.SealedTrait($name, $subtypes, $annotations))" =>
              q"_root_.com.spotify.scio.coders.Coder.dispatch(new magnolia.SealedTrait($name, $subtypes, Array()))"
            case q"""magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, $annotations)""" =>
              q"""_root_.magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, Array())"""
            case t =>
              super.transform(tree)
          }
        }
      }
    // scalastyle:on line.size.limit
    val coder = removeAnnotations.transform(getLazyVal)

    val isPrivateContructor =
      wtt.decls.collect {
        case m: MethodSymbol if m.isConstructor =>
          m.isPrivate
      }.headOption.getOrElse(false)


    val tree: c.Tree =
      if(isPrivateContructor) {
        // Magnolia does not support classes with a private constructor.
        // Workaround the limitation by usong a fallback in that case
        q"""_root_.com.spotify.scio.coders.Coder.fallback[$wtt](null)"""
      } else {
        //XXX: find a way to get rid of $outer references at compile time
        coder
      }

    tree
  }
  // scalastyle:on method.length

}
