/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.coders.instances

import java.lang.{Iterable => JIterable}

import scala.util.chaining._

private[coders] object JavaCollectionWrappers {

  // private classes
  val JIterableWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.Wrappers$JIterableWrapper")
  val JCollectionWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.Wrappers$JCollectionWrapper")
  val JListWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.Wrappers$JListWrapper")

  object JIterableWrapper {

    private lazy val underlyingField = JIterableWrapperClass
      .getDeclaredField("underlying")
      .tap(_.setAccessible(true))

    def unapply(arg: Any): Option[JIterable[_]] = arg match {
      case arg if arg.getClass == JIterableWrapperClass =>
        Some(underlyingField.get(arg).asInstanceOf[JIterable[_]])
      case _ => None
    }
  }
}
