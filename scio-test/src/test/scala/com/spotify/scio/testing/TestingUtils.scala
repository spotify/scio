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

package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

private[scio] object TestingUtils {

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] =
    Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

}
