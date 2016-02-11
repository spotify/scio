package com.spotify.scio.bigquery.types

// TODO: scala 2.11
// import scala.reflect.macros.blackbox
import org.slf4j.LoggerFactory

import scala.reflect.macros._
import scala.reflect.runtime.universe._

private[types] object MacroUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Case class helpers for runtime reflection

  def isCaseClass(t: Type): Boolean = !t.toString.startsWith("scala.") &&
    List(typeOf[Product], typeOf[Serializable], typeOf[Equals]).forall(b => t.baseClasses.contains(b.typeSymbol))
  def isField(s: Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate
  def getFields(t: Type): Iterable[Symbol] = t.declarations.filter(isField)

  // Case class helpers for macros

  // TODO: scala 2.11
  // def isCaseClass(c: blackbox.Context)(t: c.Type): Boolean = {
  def isCaseClass(c: Context)(t: c.Type): Boolean = {
    import c.universe._
    !t.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals]).forall(b => t.baseClasses.contains(b.typeSymbol))
  }
  // TODO: scala 2.11
  // def isField(c: blackbox.Context)(s: c.Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate
  // def getFields(c: blackbox.Context)(t: c.Type): Iterable[c.Symbol] = t.decls.filter(isField(c))
  def isField(c: Context)(s: c.Symbol): Boolean = !s.isSynthetic && s.isTerm && s.isPrivate
  def getFields(c: Context)(t: c.Type): Iterable[c.Symbol] = t.declarations.filter(isField(c))

  // Debugging

  def debug(msg: Any): Unit = {
    if (sys.props("bigquery.types.debug") != null && sys.props("bigquery.types.debug").toBoolean) {
      logger.info(msg.toString)
    }
  }

  // Namespace helpers

  private val SBQ = "_root_.com.spotify.scio.bigquery"
  val GModel = "_root_.com.google.api.services.bigquery.model"
  val SType = s"$SBQ.types.BigQueryType"
  val SUtil = s"$SBQ.BigQueryUtil"

  // TODO: scala 2.11
  // def p(c: blackbox.Context, code: String): c.Tree = c.parse(code)
  def p(c: Context, code: String): c.Tree = c.parse(code)

}
