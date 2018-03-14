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
import scala.reflect.runtime.universe._

class SampleValidationProvider extends ValidationProvider {
  private def getBySemanticTypeString(tfs: TableFieldSchema): Option[Class[_]] = {
    Option(tfs.getDescription)
      .flatMap(overrideType => Index.getIndexClass.get(overrideType))
  }

  private def getBySemanticTypeObject(c: blackbox.Context)
                                     (tpe: c.Type): Option[(c.Type, Class[_])] = {
    Index.getIndexCompileTimeTypes(c).find(a => a._1 =:= tpe)
  }

  private def getBySemanticTypeObject(tpe: Type): Option[(Type, Class[_])] = {
    Index.getIndexRuntimeTypes.find(a => a._1 =:= tpe)
  }

  def shouldOverrideType(tfs: TableFieldSchema): Boolean = {
    getBySemanticTypeString(tfs).nonEmpty
  }

  def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean = {
    getBySemanticTypeObject(c)(tpe).nonEmpty
  }

  def shouldOverrideType(tpe: Type): Boolean = {
    getBySemanticTypeObject(tpe).nonEmpty
  }

  def getBigQueryType(tpe: Type): String = {
    val optionalTuple = getBySemanticTypeObject(tpe)
    optionalTuple match {
      case Some(tuple) => tuple._2.getMethod ("bigQueryType").invoke (null).asInstanceOf[String]
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }

  def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree = {
    import c.universe._
    val typeClassOption: Option[Class[_]] = getBySemanticTypeString(tfs)
    typeClassOption match {
      case Some(typeClass) => val packageName = typeClass.getPackage.getName
        val className = TypeName(typeClass.getSimpleName)
        tq"${c.parse("_root_." + packageName)}.$className"
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }

  def createInstance(c: blackbox.Context)(tpe: c.Type, s: String): c.Tree = {
    import c.universe._
    val optionalTuple = getBySemanticTypeObject(c)(tpe)
    optionalTuple match {
      case Some(tuple) => q"${c.parse(tuple._2
        .getPackage.getName + "." + tuple._2.getSimpleName)}.parse($s)"
      case None => throw new IllegalArgumentException("Should never be here")
    }
  }


}
