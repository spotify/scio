package com.spotify.scio.bigquery.validation

import scala.annotation.StaticAnnotation
import scala.reflect.macros.blackbox
import scala.language.experimental.macros

/**
  * This shouldn't be necessary in most production use cases. However passing System properties from
  * Intellij can cause issues. The ideal place to set this System property is in your build.sbt
  * file.
  */
object SetProperty {

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

