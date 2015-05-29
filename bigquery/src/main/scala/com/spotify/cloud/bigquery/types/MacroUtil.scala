package com.spotify.cloud.bigquery.types

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe._

private[types] object MacroUtil {

  // TODO: deduplicate

  def isCaseClass(t: Type): Boolean = !t.toString.startsWith("scala.") &&
    List(typeOf[Product], typeOf[Serializable], typeOf[Equals]).forall(b => t.baseClasses.contains(b.typeSymbol))

  def isField(s: Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate

  def getFields(t: Type): Iterable[Symbol] = t.decls.filter(isField)

  def debug(msg: Any): Unit = {
    if (sys.props("bigquery.types.debug") != null && sys.props("bigquery.types.debug").toBoolean) {
      println(msg.toString)
    }
  }

  val SBQ = "_root_.com.spotify.cloud.bigquery"
  val SBQT = s"$SBQ.types.BigQueryType"
  val GBQM = "_root_.com.google.api.services.bigquery.model"

  def p(c: blackbox.Context, code: String): c.Tree = c.parse(code)

}
