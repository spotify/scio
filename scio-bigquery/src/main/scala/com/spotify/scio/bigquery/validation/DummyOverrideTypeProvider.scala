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

import com.google.api.services.bigquery.model.TableFieldSchema

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe

/** An [[OverrideTypeProvider]] implementation with the default behavior. */
final class DummyOverrideTypeProvider extends OverrideTypeProvider {
  override def shouldOverrideType(tfs: TableFieldSchema): Boolean = false

  override def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean = false

  override def shouldOverrideType(tpe: universe.Type): Boolean = false

  override def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree = null

  override def createInstance(c: blackbox.Context)(tpe: c.Type, tree: c.Tree): c.Tree = null

  override def getBigQueryType(tpe: universe.Type): String = null

  override def initializeToTable(c: blackbox.Context)(modifiers: c.universe.Modifiers,
                                                      variableName: c.universe.TermName,
                                                      tpe: c.universe.Tree): Unit = Unit
}
