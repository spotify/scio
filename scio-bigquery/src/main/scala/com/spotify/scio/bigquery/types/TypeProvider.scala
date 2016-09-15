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

package com.spotify.scio.bigquery.types

import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.common.base.Charsets
import com.google.common.hash.{HashCode, Hashing}
import com.google.common.io.Files
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.{BigQueryClient, BigQueryUtil}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.macros._

// scalastyle:off line.size.limit
private[types] object TypeProvider {
  private val logger = LoggerFactory.getLogger(TypeProvider.getClass)
  private lazy val bigquery: BigQueryClient = BigQueryClient.defaultInstance()

  // TODO: scala 2.11
  // def tableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
  def tableImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val args = extractStrings(c, "Missing table specification")
    val tableSpec = formatString(args)
    val schema = bigquery.getTableSchema(tableSpec)
    val traits = List(tq"${p(c, SType)}.HasTable")
    val overrides = List(q"override def table: ${p(c, GModel)}.TableReference = ${p(c, GBQIO)}.parseTableSpec($tableSpec)")

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  // TODO: scala 2.11
  // def schemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
  def schemaImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val schemaString = extractStrings(c, "Missing schema").head
    val schema = BigQueryUtil.parseSchema(schemaString)
    schemaToType(c)(schema, annottees, Nil, Nil)
  }

  // TODO: scala 2.11
  // def queryImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
  def queryImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val args = extractStrings(c, "Missing query")
    val query = formatString(args)
    val schema = bigquery.getQuerySchema(query)
    val traits = Seq(tq"${p(c, SType)}.HasQuery")
    val overrides = Seq(q"override def query: _root_.java.lang.String = ${args.head}")

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  // TODO: scala 2.11
  // def toTableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
  def toTableImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case List(q"case class $name(..$fields) { ..$body }") =>
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SType)}.schemaOf[$name]"
        val defToPrettyString = q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${name.toString}, indent)"
        val fnTrait = tq"${newTypeName(s"Function${fields.size}")}[..${fields.flatMap(_.children)}, $name]"
        val traits = if (fields.size <= 22) Seq(fnTrait) else Seq()
        val caseClassTree = q"""${caseClass(c)(name, fields, body)}"""
        (q"""$caseClassTree
            ${companion(c)(name, traits, Seq(defSchema, defToPrettyString), fields.asInstanceOf[Seq[Tree]].size)}
        """, caseClassTree, name.toString())
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
    debug(s"TypeProvider.toTableImpl:")
    debug(r)

    if (shouldDumpClassesForPlugin) { dumpCodeForScalaPlugin(c)(Seq.empty, caseClassTree, name) }

    c.Expr[Any](r)
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  // TODO: scala 2.11
  // private def schemaToType(c: blackbox.Context)
  private def schemaToType(c: Context)
                          (schema: TableSchema, annottees: Seq[c.Expr[Any]],
                           traits: Seq[c.Tree], overrides: Seq[c.Tree]): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    // Returns: (raw type, e.g. Int, String, NestedRecord, nested case class definitions)
    def getRawType(tfs: TableFieldSchema): (Tree, Seq[Tree]) = tfs.getType match {
      case "INTEGER" => (tq"_root_.scala.Long", Nil)
      case "FLOAT" => (tq"_root_.scala.Double", Nil)
      case "BOOLEAN" => (tq"_root_.scala.Boolean", Nil)
      case "STRING" => (tq"_root_.java.lang.String", Nil)
      case "TIMESTAMP" => (tq"_root_.org.joda.time.Instant", Nil)
      case "RECORD" =>
        val name = NameProvider.getUniqueName(tfs.getName)
        val (fields, records) = toFields(tfs.getFields)
        (q"${Ident(newTypeName(name))}", Seq(q"case class ${newTypeName(name)}(..$fields)") ++ records)
      case t => c.abort(c.enclosingPosition, s"type: $t not supported")
    }

    // Returns: (field type, e.g. T/Option[T]/List[T], nested case class definitions)
    def getFieldType(tfs: TableFieldSchema): (Tree, Seq[Tree]) = {
      val (t, r) = getRawType(tfs)
      val ft = tfs.getMode match {
        case "NULLABLE" | null => tq"_root_.scala.Option[$t]"
        case "REQUIRED" => t
        case "REPEATED" => tq"_root_.scala.List[$t]"
        case m => c.abort(c.enclosingPosition, s"mode: $m not supported")
      }
      (ft, r)
    }

    // Returns: ("fieldName: fieldType", nested case class definitions)
    def toField(tfs: TableFieldSchema): (Tree, Seq[Tree]) = {
      val (ft, r) = getFieldType(tfs)
      // TODO: scala 2.11
      // (q"${TermName(tfs.getName)}: $ft", r)
      (q"${newTermName(tfs.getName)}: $ft", r)
    }

    def toFields(fields: JList[TableFieldSchema]): (Seq[Tree], Seq[Tree]) = {
      val f = fields.asScala.map(s => toField(s))
      (f.map(_._1), f.flatMap(_._2))
    }

    val (fields, records) = toFields(schema.getFields)

    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case List(q"class $name") =>
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SUtil)}.parseSchema(${schema.toString})"
        val defToPrettyString = q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${name.toString}, indent)"

        val caseClassTree = q"""${caseClass(c)(name, fields, Nil)}"""
        (q"""$caseClassTree
            ${companion(c)(name, traits, Seq(defSchema, defToPrettyString) ++ overrides, fields.size)}
            ..$records
        """, caseClassTree, name.toString())
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
    debug(s"TypeProvider.schemaToType[$schema]:")
    debug(r)

    if (shouldDumpClassesForPlugin) { dumpCodeForScalaPlugin(c)(records, caseClassTree, name) }

    c.Expr[Any](r)
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length

  /** Extract string from annotation. */
  // TODO: scala 2.11
  // private def extractStrings(c: blackbox.Context, errorMessage: String): List[String] = {
  private def extractStrings(c: Context, errorMessage: String): List[String] = {
    import c.universe._

    def str(tree: c.Tree) = tree match {
      // "string literal"
      case Literal(Constant(s: String)) => s
      // "string literal".stripMargin
      // TODO: scala 2.11
      //  case Select(Literal(Constant(s: String)), TermName("stripMargin")) => s.stripMargin
      case Select(Literal(Constant(s: String)), m: TermName) if m.toString == "stripMargin" => s.stripMargin
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }

    c.macroApplication match {
      case Apply(Select(Apply(_, xs: List[_]), _), _) =>
        val args = xs.map(str)
        if (args.isEmpty) {
          c.abort(c.enclosingPosition, errorMessage)
        }
        args
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }
  }

  private def formatString(xs: List[String]): String = if (xs.tail.isEmpty) xs.head else xs.head.format(xs.tail: _*)

  /** Generate a case class. */
  // TODO: scala 2.11
  // private def caseClass(c: blackbox.Context)
  private def caseClass(c: Context)
                       (name: c.TypeName, fields: Seq[c.Tree], body: Seq[c.Tree]): c.Tree = {
    import c.universe._
    q"case class $name(..$fields) extends ${p(c, SType)}.HasAnnotation { ..$body }"
  }
  /** Generate a companion object. */
  // TODO: scala 2.11
  // private def companion(c: blackbox.Context)
  private def companion(c: Context)
                       (name: c.TypeName, traits: Seq[c.Tree], methods: Seq[c.Tree], numFields: Int): c.Tree = {
    import c.universe._

    val overrideFlag = if (traits.exists(_.toString().contains("Function"))) Flag.OVERRIDE else NoFlags
    // TODO: scala 2.11
    // val tupled = if (numFields > 1 && numFields <= 22) Seq(q"$overrideFlag def tupled = (${TermName(name.toString)}.apply _).tupled") else Nil
    val tupled = if (numFields > 1 && numFields <= 22) Seq(q"$overrideFlag def tupled = (${newTermName(name.toString)}.apply _).tupled") else Nil

    val m = converters(c)(name) ++ tupled ++ methods
    // TODO: scala 2.11
    // val tn = TermName(name.toString)
    val tn = newTermName(name.toString)
    q"""object $tn extends ${p(c, SType)}.HasSchema[$name] with ..$traits {
          ..$m
        }
    """
  }
  /** Generate override converter methods for HasSchema[T]. */
  // TODO: scala 2.11
  // private def converters(c: blackbox.Context)(name: c.TypeName): Seq[c.Tree] = {
  private def converters(c: Context)(name: c.TypeName): Seq[c.Tree] = {
    import c.universe._
    List(
      q"override def fromTableRow: (${p(c, GModel)}.TableRow => $name) = ${p(c, SType)}.fromTableRow[$name]",
      q"override def toTableRow: ($name => ${p(c, GModel)}.TableRow) = ${p(c, SType)}.toTableRow[$name]")
  }

  /** Enforce that the macro is not enclosed by a package, but a class or object instead. */
  private def checkMacroEnclosed(c: Context): Unit = {
    // TODO: scala 2.11
    //if (!c.internal.enclosingOwner.isClass) {
    // c.abort(c.enclosingPosition,
    //  "Invalid macro application - must be applied within class/object.")
    // }
    if (c.enclosingClass.isEmpty) {
      c.abort(c.enclosingPosition, s"@BigQueryType declaration must be inside a class or object.")
    }
  }

  /**
   * Check if compiler should dump generated code for Scio IDEA plugin.
   *
   * This is used to mitigate lack of support for Scala macros in IntelliJ.
   */
  private def shouldDumpClassesForPlugin = {
    sys.props("bigquery.plugin.disable.dump") == null ||
      !sys.props("bigquery.plugin.disable.dump").toBoolean
  }

  private def getBQClassCacheDir = {
    // TODO: add this as key/value settings with default etc
    if (sys.props("bigquery.class.cache.directory") != null) {
      sys.props("bigquery.class.cache.directory")
    } else {
      sys.props("java.io.tmpdir") + "/bigquery-classes"
    }
  }

  private def pShowCode(c: Context)(records: Seq[c.Tree], caseClass: c.Tree): Seq[String] = {
    // print only records and case class and do it nicely so that we can just inject those
    // in scala plugin.
    import c.universe._
    (Seq(caseClass) ++ records).map {
      case q"case class $name(..$fields) { ..$body }" =>
        s"case class $name(${fields.map{case ValDef(mods, fname, ftpt, _) => s"$fname : $ftpt"}.mkString(", ")})"
      case q"case class $name(..$fields) extends $annotation { ..$body }" =>
        s"case class $name(${fields.map{case ValDef(mods, fname, ftpt, _) => s"$fname : $ftpt"}.mkString(", ")}) extends $annotation"
      case _ => ""
    }
  }

  private def genHashForMacro(owner: String, srcFile: String): String = {
    Hashing.murmur3_32().newHasher()
      .putString(owner, Charsets.UTF_8)
      .putString(srcFile, Charsets.UTF_8)
      .hash().toString
  }

  private def dumpCodeForScalaPlugin(c: Context)(records: Seq[c.universe.Tree],
                                                 caseClassTree: c.universe.Tree,
                                                 name: String): Unit = {
    // TODO: scala 2.11
    // val owner = c.internal.enclosingOwner.fullName
    import  c.universe._
    val owner = c.enclosingClass.collect {
      case m: ModuleDef => m.symbol.fullName
      case c: ClassDef => c.symbol.fullName
    }.headOption.getOrElse {
      c.abort(c.enclosingPosition,
        "Invalid macro application - must be applied within class/object.")
    }
    val srcFile = c.macroApplication.pos.source.path
    val hash = genHashForMacro(owner, srcFile)

    // TODO scala 2.11
    // import c.universe._
    // showCode(r)
    val prettyCode = pShowCode(c)(records, caseClassTree).mkString("\n")
    val classCacheDir = getBQClassCacheDir
    val genSrcFile = new java.io.File(s"$classCacheDir/$name-$hash.scala")

    logger.info(s"Will dump generated $name of $owner from $srcFile to $genSrcFile")

    Files.createParentDirs(genSrcFile)
    Files.write(prettyCode, genSrcFile, Charsets.UTF_8)
  }

}
// scalastyle:on line.size.limit

private[types] object NameProvider {

  private val m = MMap.empty[String, Int].withDefaultValue(0)

  /**
   * Generate a unique name for a nested record.
   * This is necessary since we create case classes for nested records and name them with their
   * field names.
   */
  def getUniqueName(name: String): String = m.synchronized {
    val cName = toPascalCase(name) + '$'
    m(cName) += 1
    cName + m(cName)
  }

  private def toPascalCase(s: String): String =
    s.split('_').filter(_.nonEmpty).map(t => t(0).toUpper + t.drop(1).toLowerCase).mkString("")

}
