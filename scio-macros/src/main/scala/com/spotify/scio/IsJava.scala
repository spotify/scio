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

package com.spotify.scio

import scala.language.experimental.macros
import scala.reflect.macros._

/**
 * Proof that a type is implemented in Java
 */
sealed trait IsJava[T]

object IsJava {
  implicit def isJava[T]: IsJava[T] = macro IsJava.isJavaImpl[T]

  def apply[T](implicit i: IsJava[T]) = i

  def isJavaImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]

    if (wtt.typeSymbol.isJava)
      q"null: IsJava[$wtt]"
    else
      c.abort(c.enclosingPosition, s"$wtt is not a Java class")
  }
}
