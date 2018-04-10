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

package com.spotify.scio.bigquery.validation

import com.google.api.services.bigquery.model.TableFieldSchema

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe._

/** Interface for all custom type providers. */
trait OverrideTypeProvider {

  /**
   * Returns true if we should override default mapping.
   * Uses schema directly from BigQuery loading.
   */
  def shouldOverrideType(tfs: TableFieldSchema): Boolean

  /**
   * Returns true if we should override default mapping.
   * Uses compile time types.
   */
  def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean

  /**
   * Returns true if we should override default mapping.
   * Uses runtime types.
   */
  def shouldOverrideType(tpe: Type): Boolean

  /**
   * Returns a `context.Tree` representing the Scala type.
   * This is called at macro expansion time.
   */
  def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree

  /**
   * Returns a `context.Tree` representing a new instance of whatever type is appropriate.
   * This is called at macro expansion time.
   */
  def createInstance(c: blackbox.Context)(tpe: c.Type, tree: c.Tree): c.Tree

  /**
   * Called when we need to initialize the `toTable` method using annotations.
   * This is called at macro expansion time.
   */
  def initializeToTable(c: blackbox.Context)(modifiers: c.universe.Modifiers,
                                             variableName: c.universe.TermName,
                                             tpe: c.universe.Tree): Unit

  /**
   * Returns the `String` representation of the BigQuery column type.
   * This is called at runtime.
   */
  def getBigQueryType(tpe: Type): String
}
