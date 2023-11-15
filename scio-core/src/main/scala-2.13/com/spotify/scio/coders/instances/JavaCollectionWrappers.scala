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
private[coders] object JavaCollectionWrappers {

  // private classes
  val JIterableWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers$JIterableWrapper")
  val JCollectionWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers$JCollectionWrapper")
  val JListWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers$JListWrapper")

  object JIterableWrapper {
    def unapply(arg: Any): Option[JIterable[_]] = arg match {
      case arg if arg.getClass == JListWrapperClass =>
        val underlying = JListWrapperClass
          .getField("underlying")
          .get(arg)
          .asInstanceOf[JIterable[_]]
        Some(underlying)
      case _ => None
    }
  }
}
