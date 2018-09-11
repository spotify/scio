/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery

import scala.annotation.StaticAnnotation

package object types {

  /**
   * Case class field annotation for BigQuery field description.
   *
   * To be used with case class fields annotated with [[BigQueryType.toTable]], For example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class User(@description("user name") name: String,
   *                 @description("user age") age: Int)
   * }}}
   */
  // scalastyle:off class.name
  final class description(value: String) extends StaticAnnotation
  // scalastyle:on class.name

}
