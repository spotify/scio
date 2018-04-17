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

import java.nio.file.{Path, Paths}
import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.validation.{OverrideTypeProvider, OverrideTypeProviderFinder}
import com.spotify.scio.bigquery.{BigQueryClient, BigQueryPartitionUtil, BigQueryUtil}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.macros._

// scalastyle:off line.size.limit
private[types] object TypeProvider {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val bigquery: BigQueryClient = BigQueryClient.defaultInstance()

  def tableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val args = extractStrings(c, "Missing table specification")
    val tableSpec = BigQueryPartitionUtil.latestTable(bigquery, formatString(args))
    val schema = bigquery.getTableSchema(tableSpec)
    val traits = List(tq"${p(c, SType)}.HasTable")

    val tableDef = q"override def table: _root_.java.lang.String = ${args.head}"

    val ta =
      annottees.map(_.tree) match {
        case (q"class $cName") :: tail =>
          List(q"""
            implicit def bqTable: ${p(c, SType)}.Table[$cName] =
              new ${p(c, SType)}.Table[$cName]{
                $tableDef
              }
          """)
        case _ =>
          Nil
      }

    val overrides = List(tableDef) ++ ta
    schemaToType(c)(schema, annottees, traits, overrides)
  }

  def schemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val schemaString = extractStrings(c, "Missing schema").head
    val schema = BigQueryUtil.parseSchema(schemaString)
    schemaToType(c)(schema, annottees, Nil, Nil)
  }

  def queryImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val args = extractStrings(c, "Missing query")
    val query = BigQueryPartitionUtil.latestQuery(bigquery, formatString(args))
    val schema = bigquery.getQuerySchema(query)
    val traits = List(tq"${p(c, SType)}.HasQuery")

    val queryDef = q"override def query: _root_.java.lang.String = ${args.head}"

    val qa =
      annottees.map(_.tree) match {
        case (q"class $cName") :: tail =>
          List(q"""
            implicit def bqQuery: ${p(c, SType)}.Query[$cName] =
              new ${p(c, SType)}.Query[$cName]{
                $queryDef
              }
          """)
        case _ =>
          Nil
      }
    val overrides = List(queryDef) ++ qa

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  private def getTableDescription(c: blackbox.Context)(cd: c.universe.ClassDef)
  : List[c.universe.Tree] = {
    cd.mods.annotations
      .filter(_.children.head.toString().matches("^new description$"))
      .map(_.children.tail.head)
  }


  def toTableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider
    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case (clazzDef @ q"$mods class $cName[..$tparams] $ctorMods(..$fields) extends { ..$earlydefns } with ..$parents { $self => ..$body }") :: tail if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        if (parents.map(_.toString()).toSet != Set("scala.Product", "scala.Serializable")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the case class $clazzDef")
        }
        val desc = getTableDescription(c)(clazzDef.asInstanceOf[ClassDef])
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SType)}.schemaOf[$cName]"
        val defTblDesc = desc.headOption.map(d => q"override def tableDescription: _root_.java.lang.String = $d")
        val defToPrettyString = q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${cName.toString}, indent)"
        val fnTrait = tq"${TypeName(s"Function${fields.size}")}[..${fields.map(_.children.head)}, $cName]"
        val traits = (if (fields.size <= 22) Seq(fnTrait) else Seq()) ++ defTblDesc.map(_ => tq"${p(c, SType)}.HasTableDescription")
        val taggedFields = fields.map {
          case ValDef(m, n, tpt, rhs) => {
            provider.initializeToTable(c)(m, n, tpt)
            c.universe.ValDef(c.universe.Modifiers(m.flags, m.privateWithin, m.annotations), n, tq"$tpt @${typeOf[BigQueryTag]}", rhs)
          }
        }
        val caseClassTree = q"""${caseClass(c)(mods, cName, taggedFields, body)}"""
        val maybeCompanion = tail.headOption
        (q"""$caseClassTree
            ${companion(c)(cName, traits, Seq(defSchema, defToPrettyString) ++ defTblDesc, taggedFields.asInstanceOf[Seq[Tree]].size, maybeCompanion)}
        """, caseClassTree, cName.toString())
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
    debug(s"TypeProvider.toTableImpl:")
    debug(r)

    if (shouldDumpClassesForPlugin) { dumpCodeForScalaPlugin(c)(Seq.empty, caseClassTree, name) }

    c.Expr[Any](r)
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  private def schemaToType(c: blackbox.Context)
                          (schema: TableSchema, annottees: Seq[c.Expr[Any]],
                           traits: Seq[c.Tree], overrides: Seq[c.Tree]): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider

    // Returns: (raw type, e.g. Int, String, NestedRecord, nested case class definitions)
    def getRawType(tfs: TableFieldSchema): (Tree, Seq[Tree]) = tfs.getType match {
      case t if provider.shouldOverrideType(tfs) => (provider.getScalaType(c)(tfs), Nil)
      case "BOOLEAN" | "BOOL" => (tq"_root_.scala.Boolean", Nil)
      case "INTEGER" | "INT64" => (tq"_root_.scala.Long", Nil)
      case "FLOAT" | "FLOAT64" => (tq"_root_.scala.Double", Nil)
      case "STRING" => (tq"_root_.java.lang.String", Nil)
      case "BYTES" => (tq"_root_.com.google.protobuf.ByteString", Nil)
      case "TIMESTAMP" => (tq"_root_.org.joda.time.Instant", Nil)
      case "DATE" => (tq"_root_.org.joda.time.LocalDate", Nil)
      case "TIME" => (tq"_root_.org.joda.time.LocalTime", Nil)
      case "DATETIME" => (tq"_root_.org.joda.time.LocalDateTime", Nil)
      case "RECORD" | "STRUCT" =>
        val name = NameProvider.getUniqueName(tfs.getName)
        val (fields, records) = toFields(tfs.getFields)
        (q"${Ident(TypeName(name))}", Seq(q"case class ${TypeName(name)}(..$fields)") ++ records)
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
      val params = q"val ${TermName(tfs.getName)}: $ft @${typeOf[BigQueryTag]}"
      (params, r)
    }

    def toFields(fields: JList[TableFieldSchema]): (Seq[Tree], Seq[Tree]) = {
      val f = fields.asScala.map(s => toField(s))
      (f.map(_._1), f.flatMap(_._2))
    }

    val (fields, records) = toFields(schema.getFields)

    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case (clazzDef @ q"$mods class $cName[..$tparams] $ctorMods(..$cfields) extends { ..$earlydefns } with ..$parents { $self => ..$body }") :: tail if mods.asInstanceOf[Modifiers].flags == NoFlags =>
        if (parents.map(_.toString()).toSet != Set("scala.AnyRef")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the class $clazzDef")
        }
        if (cfields.nonEmpty) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't provide class fields $clazzDef")
        }

        val desc = getTableDescription(c)(clazzDef.asInstanceOf[ClassDef])
        val defTblDesc = desc.headOption.map(d => q"override def tableDescription: _root_.java.lang.String = $d")
        val defTblTrait = defTblDesc.map(_ => tq"${p(c, SType)}.HasTableDescription").toSeq
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SUtil)}.parseSchema(${schema.toString})"
        val defToPrettyString = q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${cName.toString}, indent)"

        val caseClassTree = q"""${caseClass(c)(mods, cName, fields, body)}"""
        val maybeCompanion = tail.headOption
        (q"""$caseClassTree
            ${companion(c)(cName, traits ++ defTblTrait, Seq(defSchema, defToPrettyString) ++ overrides ++ defTblDesc, fields.size, maybeCompanion)}
            ..$records
        """, caseClassTree, cName.toString)
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
  private def extractStrings(c: blackbox.Context, errorMessage: String): List[String] = {
    import c.universe._

    def str(tree: c.Tree) = tree match {
      // "string literal"
      case Literal(Constant(s: String)) => s
      // "string literal".stripMargin
      case Select(Literal(Constant(s: String)), TermName("stripMargin")) => s.stripMargin
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
  private def caseClass(c: blackbox.Context)
                       (mods: c.Modifiers, name: c.TypeName, fields: Seq[c.Tree], body: Seq[c.Tree]): c.Tree = {
    import c.universe._
    val tagAnnot = q"new _root_.com.spotify.scio.bigquery.types.BigQueryTag"
    val taggedMods = Modifiers(Flag.CASE, typeNames.EMPTY, tagAnnot :: mods.annotations)
    q"$taggedMods class $name(..$fields) extends ${p(c, SType)}.HasAnnotation { ..$body }"
  }
  /** Generate a companion object. */
  private def companion(c: blackbox.Context)
                       (name: c.TypeName, traits: Seq[c.Tree], methods: Seq[c.Tree], numFields: Int, originalCompanion: Option[c.Tree]): c.Tree = {
    import c.universe._

    val overrideFlag = if (traits.exists(_.toString().contains("Function"))) Flag.OVERRIDE else NoFlags
    val tupled = if (numFields > 1 && numFields <= 22) Seq(q"$overrideFlag def tupled = (${TermName(name.toString)}.apply _).tupled") else Nil

    val m = converters(c)(name) ++ tupled ++ methods
    val tn = TermName(name.toString)

    if (originalCompanion.isDefined) {
      val q"$mods object $cName extends { ..$_ } with ..$parents { $_ => ..$body }" = originalCompanion.get
      // need to filter out Object, otherwise get duplicate Object error
      // also can't get a FQN of scala.AnyRef which gets erased to java.lang.Object, can't find a
      // sane way to =:= scala.AnyRef
      val filteredTraits = (traits ++ parents).toSet.filterNot(_.toString == "scala.AnyRef")
      q"""$mods object $cName extends ${p(c, SType)}.HasSchema[$name] with ..$filteredTraits {
          ..${body ++ m}
        }"""
    } else {
      q"""object $tn extends ${p(c, SType)}.HasSchema[$name] with ..$traits {
          ..$m
        }"""
    }
  }
  /** Generate override converter methods for HasSchema[T]. */
  private def converters(c: blackbox.Context)(name: c.TypeName): Seq[c.Tree] = {
    import c.universe._
    List(
      q"override def fromAvro: (_root_.org.apache.avro.generic.GenericRecord => $name) = ${p(c, SType)}.fromAvro[$name]",
      q"override def fromTableRow: (${p(c, GModel)}.TableRow => $name) = ${p(c, SType)}.fromTableRow[$name]",
      q"override def toTableRow: ($name => ${p(c, GModel)}.TableRow) = ${p(c, SType)}.toTableRow[$name]")
  }

  /** Enforce that the macro is not enclosed by a package, but a class or object instead. */
  private def checkMacroEnclosed(c: blackbox.Context): Unit = {
    val owner = c.internal.enclosingOwner
    if (owner.isPackage || !owner.isClass) {
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

  private def getBQClassCacheDir: Path = {
    // TODO: add this as key/value settings with default etc
    if (sys.props("bigquery.class.cache.directory") != null) {
      Paths.get(sys.props("bigquery.class.cache.directory"))
    } else {
      Paths.get(sys.props("java.io.tmpdir"))
        .resolve(sys.props("user.name"))
        .resolve("bigquery-classes")
    }
  }

  // scalastyle:off line.size.limit
  private def pShowCode(c: blackbox.Context)(records: Seq[c.Tree], caseClass: c.Tree): Seq[String] = {
    // print only records and case class and do it nicely so that we can just inject those
    // in scala plugin.
    import c.universe._
    (Seq(caseClass) ++ records).map {
      case q"$mods class $name[..$_] $_(..$fields) extends { ..$_ } with ..$parents { $_ => ..$_ }" if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        val f = fields.map {
          case ValDef(_, fname, ftpt, _) => s"${SchemaUtil.escapeNameIfReserved(fname.toString)} : $ftpt"
        }.mkString(", ")
          .replaceAll(s"@${classOf[BigQueryTag].getName}", "") //BQ plugin does not need to know about BQTag
        parents match {
          case Nil =>
            s"case class $name($f)"
          case h :: Nil =>
            s"case class $name($f) extends $h"
          case h :: t =>
            s"case class $name($f) extends $h ${t.mkString(" with ", " with ", "")}"
        }
      case _ => ""
    }
  }
  // scalastyle:on line.size.limit

  private def genHashForMacro(owner: String, srcFile: String): String = {
    Hashing.murmur3_32().newHasher()
      .putString(owner, Charsets.UTF_8)
      .putString(srcFile, Charsets.UTF_8)
      .hash().toString
  }

  private def dumpCodeForScalaPlugin(c: blackbox.Context)(records: Seq[c.universe.Tree],
                                                 caseClassTree: c.universe.Tree,
                                                 name: String): Unit = {
    val owner = c.internal.enclosingOwner.fullName
    val srcFile = c.macroApplication.pos.source.file.canonicalPath
    val hash = genHashForMacro(owner, srcFile)

    val prettyCode = pShowCode(c)(records, caseClassTree).mkString("\n")
    val classCacheDir = getBQClassCacheDir
    val genSrcFile = classCacheDir.resolve(s"$name-$hash.scala").toFile

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
