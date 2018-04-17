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


package com.spotify.scio.bigquery.validation

import scala.collection.mutable
import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe._

// Internal index to keep track of class mappings this can be done in a number of ways
object Index {
  def getIndexCompileTimeTypes(c: blackbox.Context): mutable.Map[c.Type, Class[_]] = {
    import c.universe._
    mutable.Map[Type, Class[_]](typeOf[Country] -> classOf[Country])
  }

  def getIndexClass: mutable.Map[String, Class[_]] = {
    mutable.Map[String, Class[_]](Country.stringType -> classOf[Country])
  }

  def getIndexRuntimeTypes: mutable.Map[Type, Class[_]] = {
    mutable.Map[Type, Class[_]](typeOf[Country] -> classOf[Country])
  }

}
