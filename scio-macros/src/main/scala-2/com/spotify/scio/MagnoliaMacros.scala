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

package com.spotify.scio

import scala.reflect.macros._

private[scio] object MagnoliaMacros {
  import magnolia1._

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def genWithoutAnnotations[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]

    if (wtt <:< typeOf[Iterable[_]]) {
      c.abort(
        c.enclosingPosition,
        s"Automatic coder derivation can't derive a Coder for $wtt <: Seq"
      )
    }

    val magnoliaTree = Magnolia.gen[T](c)

    // format: off
    // Remove annotations from magnolia since they are
    // not serializable and we don't use them anyway
    val removeAnnotations: PartialFunction[Tree, Tree] = {
      case q"$caseClass($typeName, $isObject, $isValueClass, $parametersArray, $_, $_, $_)" if caseClass.symbol.name == TypeName("CaseClass") =>
        q"$caseClass($typeName, $isObject, $isValueClass, $parametersArray, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
      case q"Param.apply[$tpTC, $tpT, $tpP]($name, $typeNameParam, $idx, $isRepeated, $typeclassParam, $defaultVal, $_, $_, $_)" =>
        q"_root_.magnolia1.Param[$tpTC, $tpT, $tpP]($name, $typeNameParam, $idx, $isRepeated, $typeclassParam, $defaultVal, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
      case q"new SealedTrait($typeName, $subtypesArray, $_, $_, $_)" =>
        q"new _root_.magnolia1.SealedTrait($typeName, $subtypesArray, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
      case q"Subtype[$tpTC, $tpT, $tpS]($name, $idx, $_, $_, $_, $tc, $isType, $asType)" =>
        q"_root_.magnolia1.Subtype[$tpTC, $tpT, $tpS]($name, $idx, Array.empty[Any], Array.empty[Any], Array.empty[Any], $tc, $isType, $asType)"
    }

    // remove all outer references used in rawConstruct
    // so method serialization do not take any closure
    val inlineRawConstruct: PartialFunction[Tree, Tree] = {
      case q"def rawConstruct(..$params): $tpT = {..$statements}" =>
        val closureCleansed = if (statements.size > 1) statements.tail else statements
        q"def rawConstruct(..$params): $tpT = {..$closureCleansed}"
    }
    // format: on

    val scioTransformer = new Transformer {
      override def transform(tree: Tree): Tree =
        super.transform(
          removeAnnotations.orElse(inlineRawConstruct).applyOrElse[Tree, Tree](tree, t => t)
        )
    }

    scioTransformer.transform(magnoliaTree)
  }
}
