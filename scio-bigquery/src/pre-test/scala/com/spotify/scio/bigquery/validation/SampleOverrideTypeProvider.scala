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

// A sample implementation to override types under certain conditions
class SampleOverrideTypeProvider extends OverrideTypeProvider {

  private def getByTypeString(tfs: TableFieldSchema): Option[Class[_]] = {
    Option(tfs.getDescription)
      .flatMap(overrideType => Index.getIndexClass.get(overrideType))
  }

  private def getByTypeObject(c: blackbox.Context)
                             (tpe: c.Type): Option[(c.Type, Class[_])] = {
    Index.getIndexCompileTimeTypes(c).find(a => {
      val (compileTimeType, _) = a
      compileTimeType =:= tpe
    })
  }

  private def getByTypeObject(tpe: Type): Option[(Type, Class[_])] = {
    Index.getIndexRuntimeTypes.find(a => {
      val (runtimeType, _) = a
      runtimeType =:= tpe
    })
  }

  def shouldOverrideType(tfs: TableFieldSchema): Boolean = {
    getByTypeString(tfs).nonEmpty
  }

  def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean = {
    getByTypeObject(c)(tpe).nonEmpty
  }

  def shouldOverrideType(tpe: Type): Boolean = {
    getByTypeObject(tpe).nonEmpty
  }

  def getBigQueryType(tpe: Type): String = {
    val optionalTuple = getByTypeObject(tpe)
    optionalTuple match {
      case Some(tuple) =>
        val (_, correspondingType) = tuple
        correspondingType.getMethod("bigQueryType").invoke(null).asInstanceOf[String]
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }

  def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree = {
    import c.universe._
    val typeClassOption: Option[Class[_]] = getByTypeString(tfs)
    typeClassOption match {
      case Some(typeClass) => val packageName = typeClass.getPackage.getName
        val className = TypeName(typeClass.getSimpleName)
        tq"${c.parse("_root_." + packageName)}.$className"
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }

  def createInstance(c: blackbox.Context)(tpe: c.Type, tree: c.Tree): c.Tree = {
    import c.universe._
    val optionalTuple = getByTypeObject(c)(tpe)
    optionalTuple match {
      case Some(tuple) =>
        val (_, correspondingType) = tuple
        val instanceOfType = q"${
          c.parse(correspondingType
            .getPackage.getName + "." + correspondingType.getSimpleName)
        }.parse($tree)"
        instanceOfType
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }

  def initializeToTable(c: blackbox.Context)(modifiers: c.universe.Modifiers,
                                             variableName: c.universe.TermName,
                                             tpe: c.universe.Tree): Unit = Unit
}
