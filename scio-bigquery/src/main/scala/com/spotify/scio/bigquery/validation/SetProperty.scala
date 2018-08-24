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

import scala.annotation.StaticAnnotation
import scala.reflect.macros.blackbox
import scala.language.experimental.macros

// This shouldn't be necessary in most production use cases. However passing System properties from
// Intellij can cause issues. The ideal place to set this system property is in your build.sbt file.
private[validation] object SetProperty {

  class setProperty extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro setPropertyImpl
  }

  def setSystemProperty(): Unit = System.setProperty("override.type.provider",
    "com.spotify.scio.bigquery.validation.SampleOverrideTypeProvider")

  def setPropertyImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    setSystemProperty()
    annottees.head
  }
}

