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

package com.spotify.scio.extra.function

import java.util.function.{BiPredicate, Function => JFunction, Predicate => JPredicate}

import scala.language.implicitConversions

/**
 * implicits for converting between scala and java8 functions, predicates, etc.
 */
object JavaConverters {

  implicit def toJavaFunction[A, B](f: (A) => B): JFunction[A, B] = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  implicit def toJavaPredicate[A](f: (A) => Boolean): JPredicate[A] = new JPredicate[A] {
    override def test(a: A): Boolean = f(a)
  }

  implicit def toJavaBiPredicate[A, B](predicate: (A, B) ⇒ Boolean): BiPredicate[A, B] =
    new BiPredicate[A, B] {
      def test(a: A, b: B) = predicate(a, b)
    }

}
