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

    val magnoliaTree = magnolia.Magnolia.gen[T](c)

    // Remove annotations from magnolia since they are
    // not serializable and we don't use them anyway

    val removeAnnotations = new Transformer {
      override def transform(tree: Tree): c.universe.Tree =
        tree match {
          case Apply(
              AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps),
              List(typeName, isObject, isValueClass, params, annotations)
              ) =>
            val t2 = Apply(
              AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps),
              List(typeName, isObject, isValueClass, params, q"""Array()""")
            )
            super.transform(t2)
          case q"""magnolia.Param.apply[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, $annotations)""" =>
            val t2 =
              q"""_root_.magnolia.Param.apply[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, Array())"""
            super.transform(t2)
          case q"""new magnolia.SealedTrait($typeName, $subtypes, $annotations)""" =>
            val t2 = q"""new _root_.magnolia.SealedTrait($typeName, $subtypes, Array())"""
            super.transform(t2)
          case q"""magnolia.Subtype.apply[$tc, $t, $p]($typeName, $id, $annotations, $coder, $cast0, $cast1)""" =>
            val t2 =
              q"""_root_.magnolia.Subtype.apply[$tc, $t, $p]($typeName, $id, Array(), $coder, $cast0, $cast1)"""
            super.transform(t2)
          case t =>
            println(t)
            super.transform(t)
        }
    }

    removeAnnotations.transform(magnoliaTree)
  }
}
