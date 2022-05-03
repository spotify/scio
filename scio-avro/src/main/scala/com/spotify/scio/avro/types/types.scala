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

package com.spotify.scio.avro

import scala.annotation.{StaticAnnotation, nowarn}

package object types {

  /**
   * Case class and argument annotation to get Avro field and record docs.
   *
   * To be used with case class fields annotated with [[AvroType.toSchema]], For example:
   *
   * Example:
   *
   * {{{
   * AvroType.toSchema
   * @doc("User Record")
   * case class User(@doc("user name") name: String,
   *                 @doc("user age") age: Int)
   * }}}
   */
  @nowarn("msg=parameter value value in class doc is never used")
  class doc(value: String) extends StaticAnnotation
}
