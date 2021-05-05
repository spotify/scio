/*
 * Copyright 2019 Spotify AB.
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

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.CoreSysProps
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.validation.{OverrideTypeProvider, OverrideTypeProviderFinder}
import com.spotify.scio.bigquery.{
  BigQueryPartitionUtil,
  BigQuerySysProps,
  BigQueryUtil,
  StorageUtil
}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap}
import scala.reflect.macros._

private[types] object TypeProvider {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val bigquery: BigQuery = BigQuery.defaultInstance()

  def tableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val args = extractArgs(c) match {
      case Nil => c.abort(c.enclosingPosition, "Missing table specification")
      case l   => l
    }
    val (table: String, _) :: _ = args
    val tableSpec =
      BigQueryPartitionUtil.latestTable(bigquery, formatString(args.map(_._1)))
    val schema = bigquery.tables.schema(tableSpec)
    val traits = List(tq"${p(c, SType)}.HasTable")

    val tableDef = q"override def table: _root_.java.lang.String = $table"

    val ta =
      annottees.map(_.tree) match {
        case q"class $cName" :: _ =>
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
    val (schemaString: String, _) :: _ = extractArgs(c) match {
      case Nil => c.abort(c.enclosingPosition, "Missing schema")
      case l   => l
    }
    val schema = BigQueryUtil.parseSchema(schemaString)
    schemaToType(c)(schema, annottees, Nil, Nil)
  }

  def storageImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val (table, args, selectedFields, rowRestriction) = extractStorageArgs(c)
    val tableSpec = BigQueryPartitionUtil.latestTable(bigquery, formatString(table :: args))
    val avroSchema = bigquery.tables.storageReadSchema(tableSpec, selectedFields, rowRestriction)
    val schema = StorageUtil.toTableSchema(avroSchema)
    val traits = List(tq"${p(c, SType)}.HasStorageOptions")
    val overrides = List(
      q"override def table: _root_.java.lang.String = $table",
      q"override def selectedFields: _root_.scala.List[_root_.java.lang.String] = _root_.scala.List(..$selectedFields)",
      q"override def rowRestriction: _root_.scala.Option[_root_.java.lang.String] = $rowRestriction"
    )

    val ta =
      annottees.map(_.tree) match {
        case q"class $cName" :: _ =>
          List(q"""
            implicit def bqStorage: ${p(c, SType)}.StorageOptions[$cName] =
              new ${p(c, SType)}.StorageOptions[$cName]{
                ..$overrides
              }
          """)
        case _ =>
          Nil
      }

    schemaToType(c)(schema, annottees, traits, overrides ++ ta)
  }

  def queryImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val extractedArgs = extractArgs(c) match {
      case Nil => c.abort(c.enclosingPosition, "Missing query")
      case l   => l
    }
    val (queryFormat: String, _) :: queryArgs = extractedArgs
    val query = BigQueryPartitionUtil.latestQuery(bigquery, formatString(extractedArgs.map(_._1)))
    val schema = bigquery.query.schema(query)
    val traits = List(tq"${p(c, SType)}.HasQuery")

    val queryDef =
      q"override def query: _root_.java.lang.String = $queryFormat"

    val queryRawDef =
      q"override def queryRaw: _root_.java.lang.String = $queryFormat"

    val queryArgTypes = queryArgs.map(t => t._2 -> TermName(c.freshName("queryArg$")))
    val queryFnDef = if (queryArgTypes.nonEmpty) {
      val typesQ = queryArgTypes.map { case (tpt, termName) => q"$termName: $tpt" }
      val queryFn = q"""
        def query(..$typesQ): String = $queryFormat.format(..${queryArgTypes.map(_._2)})
      """

      val queryAsSource = q"""
        def queryAsSource(..$typesQ): ${p(c, SBQ)}.Query =
          ${p(c, SBQ)}.Query(query(..${queryArgTypes.map(_._2)}))
      """
      List(queryFn, queryAsSource)
    } else {
      List.empty[c.Tree]
    }

    val qa =
      annottees.map(_.tree) match {
        case q"class $cName" :: _ =>
          List(q"""
            implicit def bqQuery: ${p(c, SType)}.Query[$cName] =
              new ${p(c, SType)}.Query[$cName]{
                $queryDef
                $queryRawDef
              }
          """)
        case _ =>
          Nil
      }
    val overrides = queryFnDef ::: queryDef :: queryRawDef :: qa

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  private def getTableDescription(
    c: blackbox.Context
  )(cd: c.universe.ClassDef): List[c.universe.Tree] =
    cd.mods.annotations
      .filter(_.children.head.toString.matches("^new description$"))
      .map(_.children.tail.head)

  def toTableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider
    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case (clazzDef @ q"$mods class $cName[..$_] $_(..$fields) extends { ..$_ } with ..$parents { $_ => ..$body }") :: tail
          if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        if (parents.map(_.toString).toSet != Set("scala.Product", "scala.Serializable")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the case class $clazzDef")
        }
        val desc = getTableDescription(c)(clazzDef.asInstanceOf[ClassDef])
        val defSchema =
          q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SType)}.schemaOf[$cName]"
        val defAvroSchema =
          q"override def avroSchema: org.apache.avro.Schema =  ${p(c, SType)}.avroSchemaOf[$cName]"
        val defTblDesc =
          desc.headOption.map(d => q"override def tableDescription: _root_.java.lang.String = $d")
        val defToPrettyString =
          q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${cName.toString}, indent)"
        val fnTrait =
          tq"${TypeName(s"Function${fields.size}")}[..${fields.map(_.children.head)}, $cName]"
        val traits = (if (fields.size <= 22) Seq(fnTrait) else Seq()) ++ defTblDesc
          .map(_ => tq"${p(c, SType)}.HasTableDescription")
        val taggedFields = fields.map {
          case q"$m val $n: _root_.com.spotify.scio.bigquery.types.Geography = $rhs" =>
            provider.initializeToTable(c)(m, n, tq"_root_.java.lang.String")
            c.universe.ValDef(
              c.universe.Modifiers(m.flags, m.privateWithin, m.annotations),
              n,
              tq"_root_.java.lang.String @${typeOf[BigQueryTag]}",
              q"{$rhs}.wkt"
            )
          case ValDef(m, n, tpt, rhs) =>
            provider.initializeToTable(c)(m, n, tpt)
            c.universe.ValDef(
              c.universe.Modifiers(m.flags, m.privateWithin, m.annotations),
              n,
              tq"$tpt @${typeOf[BigQueryTag]}",
              rhs
            )
        }
        val caseClassTree =
          q"""${caseClass(c)(mods, cName, taggedFields, body)}"""
        val maybeCompanion = tail.headOption
        (
          q"""$caseClassTree
            ${companion(c)(
            cName,
            traits,
            Seq(defSchema, defAvroSchema, defToPrettyString) ++ defTblDesc,
            taggedFields.asInstanceOf[Seq[Tree]].size,
            maybeCompanion
          )}
        """,
          caseClassTree,
          cName.toString
        )
      case t =>
        val error =
          s"""Invalid annotation:
             |
             |Refer to https://spotify.github.io/scio/api/com/spotify/scio/bigquery/types/BigQueryType$$$$toTable.html
             |for details on how to use `@BigQueryType.toTable`
             |
             |>> $t
          """.stripMargin

        c.abort(c.enclosingPosition, error)
    }
    debug(s"TypeProvider.toTableImpl:")
    debug(r)

    if (shouldDumpClassesForPlugin) {
      dumpCodeForScalaPlugin(c)(Seq.empty, caseClassTree, name)
    }

    c.Expr[Any](r)
  }

  private def schemaToType(c: blackbox.Context)(
    schema: TableSchema,
    annottees: Seq[c.Expr[Any]],
    traits: Seq[c.Tree],
    overrides: Seq[c.Tree]
  ): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val provider: OverrideTypeProvider = OverrideTypeProviderFinder.getProvider

    // Returns: (raw type, e.g. Int, String, NestedRecord, nested case class definitions)
    def getRawType(tfs: TableFieldSchema): (Tree, Seq[Tree]) =
      tfs.getType match {
        case _ if provider.shouldOverrideType(tfs) =>
          (provider.getScalaType(c)(tfs), Nil)
        case "BOOLEAN" | "BOOL"  => (tq"_root_.scala.Boolean", Nil)
        case "INTEGER" | "INT64" => (tq"_root_.scala.Long", Nil)
        case "FLOAT" | "FLOAT64" => (tq"_root_.scala.Double", Nil)
        case "STRING"            => (tq"_root_.java.lang.String", Nil)
        case "NUMERIC"           => (tq"_root_.scala.BigDecimal", Nil)
        case "BYTES"             => (tq"_root_.com.google.protobuf.ByteString", Nil)
        case "TIMESTAMP"         => (tq"_root_.org.joda.time.Instant", Nil)
        case "DATE"              => (tq"_root_.org.joda.time.LocalDate", Nil)
        case "TIME"              => (tq"_root_.org.joda.time.LocalTime", Nil)
        case "DATETIME"          => (tq"_root_.org.joda.time.LocalDateTime", Nil)
        case "GEOGRAPHY" =>
          (tq"_root_.com.spotify.scio.bigquery.types.Geography", Nil)
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
        case "REQUIRED"        => t
        case "REPEATED"        => tq"_root_.scala.List[$t]"
        case m                 => c.abort(c.enclosingPosition, s"mode: $m not supported")
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
      (f.map(_._1).toSeq, f.flatMap(_._2).toSeq)
    }

    val (fields, records) = toFields(schema.getFields)

    val (r, caseClassTree, name) = annottees.map(_.tree) match {
      case (clazzDef @ q"$mods class $cName[..$_] $_(..$cfields) extends { ..$_ } with ..$parents { $_ => ..$body }") :: tail
          if mods.asInstanceOf[Modifiers].flags == NoFlags =>
        if (parents.map(_.toString).toSet != Set("scala.AnyRef")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the class $clazzDef")
        }
        if (cfields.nonEmpty) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't provide class fields $clazzDef")
        }

        val desc = getTableDescription(c)(clazzDef.asInstanceOf[ClassDef])
        val defTblDesc =
          desc.headOption.map(d => q"override def tableDescription: _root_.java.lang.String = $d")
        val defTblTrait =
          defTblDesc.map(_ => tq"${p(c, SType)}.HasTableDescription").toSeq
        val defSchema = {
          schema.setFactory(new JacksonFactory)
          q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SUtil)}.parseSchema(${schema.toString})"
        }
        val defAvroSchema =
          q"override def avroSchema: org.apache.avro.Schema = ${p(c, BigQueryUtils)}.toGenericAvroSchema(${cName.toString}, this.schema.getFields)"
        val defToPrettyString =
          q"override def toPrettyString(indent: Int = 0): String = ${p(c, s"$SBQ.types.SchemaUtil")}.toPrettyString(this.schema, ${cName.toString}, indent)"

        val caseClassTree = q"""${caseClass(c)(mods, cName, fields, body)}"""
        val maybeCompanion = tail.headOption
        (
          q"""$caseClassTree
            ${companion(c)(
            cName,
            traits ++ defTblTrait,
            Seq(defSchema, defAvroSchema, defToPrettyString) ++ overrides ++ defTblDesc,
            fields.size,
            maybeCompanion
          )}
            ..$records
        """,
          caseClassTree,
          cName.toString
        )
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
    debug(s"TypeProvider.schemaToType[$schema]:")
    debug(r)

    if (shouldDumpClassesForPlugin) {
      dumpCodeForScalaPlugin(c)(records, caseClassTree, name)
    }

    c.Expr[Any](r)
  }

  /** Extract string from annotation. */
  private def extractArgs(c: blackbox.Context): List[(Any, c.universe.Type)] = {
    import c.universe._

    def str(tree: c.Tree) = tree match {
      // "argument literal"
      case Literal(Constant(arg @ (_: String))) => (arg, typeOf[String])
      case Literal(Constant(arg @ (_: Float)))  => (arg, typeOf[Float])
      case Literal(Constant(arg @ (_: Double))) => (arg, typeOf[Double])
      case Literal(Constant(arg @ (_: Int)))    => (arg, typeOf[Int])
      case Literal(Constant(arg @ (_: Long)))   => (arg, typeOf[Long])
      // "string literal".stripMargin
      case Select(Literal(Constant(s: String)), TermName("stripMargin")) =>
        (s.stripMargin, typeOf[String])
      case arg => c.abort(c.enclosingPosition, s"Unsupported argument $arg")
    }

    c.macroApplication match {
      case Apply(Select(Apply(_, xs: List[_]), _), _) => xs.map(str(_))
      case _                                          => Nil
    }
  }

  private def extractStorageArgs(
    c: blackbox.Context
  ): (String, List[String], List[String], Option[String]) = {
    import c.universe._

    def str(tree: c.Tree) = tree match {
      // "argument literal"
      case Literal(Constant(arg @ (_: String))) => arg
      // "string literal".stripMargin
      case Select(Literal(Constant(s: String)), TermName("stripMargin")) => s.stripMargin
      case arg                                                           => c.abort(c.enclosingPosition, s"Unsupported argument $arg")
    }

    val posList = MBuffer.empty[List[String]]
    val namedArgs = MMap.empty[String, List[String]]

    c.macroApplication match {
      case Apply(Select(Apply(_, xs: List[_]), _), _) =>
        val table = str(xs.head)
        xs.tail.foreach {
          case q"args = List(..$xs)"           => namedArgs("args") = xs.map(str)
          case q"selectedFields = List(..$xs)" => namedArgs("selectedFields") = xs.map(str)
          case q"rowRestriction = $s"          => namedArgs("rowRestriction") = List(str(s))
          case q"List(..$xs)"                  => posList += xs.map(str)
          case q"$s"                           => posList += List(str(s))
        }
        val posArgs = List("args", "selectedFields", "rowRestriction").zip(posList).toMap
        val dups = posArgs.keySet intersect namedArgs.keySet
        if (dups.nonEmpty) {
          c.abort(c.enclosingPosition, s"Duplicate arguments ${dups.mkString(", ")}")
        }
        val argMap = posArgs ++ namedArgs
        val args = argMap.getOrElse("args", Nil)
        val selectedFields = argMap.getOrElse("selectedFields", Nil)
        val rowRestriction = argMap.getOrElse("rowRestriction", Nil).headOption
        (table, args, selectedFields, rowRestriction)
    }
  }

  private def formatString(xs: List[Any]): String =
    xs match {
      case (head: String) :: Nil  => head
      case (head: String) :: tail => head.format(tail: _*)
      case _                      => ""
    }

  /** Generate a case class. */
  private def caseClass(
    c: blackbox.Context
  )(mods: c.Modifiers, name: c.TypeName, fields: Seq[c.Tree], body: Seq[c.Tree]): c.Tree = {
    import c.universe._
    val tagAnnot = q"new _root_.com.spotify.scio.bigquery.types.BigQueryTag"
    val taggedMods =
      Modifiers(Flag.CASE, typeNames.EMPTY, tagAnnot :: mods.annotations)
    q"$taggedMods class $name(..$fields) extends ${p(c, SType)}.HasAnnotation { ..$body }"
  }

  /** Generate a companion object. */
  private def companion(c: blackbox.Context)(
    name: c.TypeName,
    traits: Seq[c.Tree],
    methods: Seq[c.Tree],
    numFields: Int,
    originalCompanion: Option[c.Tree]
  ): c.Tree = {
    import c.universe._

    val overrideFlag =
      if (traits.exists(_.toString.contains("Function"))) Flag.OVERRIDE
      else NoFlags
    val tupled =
      if (numFields > 1 && numFields <= 22)
        Seq(q"$overrideFlag def tupled = (${TermName(name.toString)}.apply _).tupled")
      else Nil

    val m = converters(c)(name) ++ tupled ++ methods
    val tn = TermName(name.toString)

    if (originalCompanion.isDefined) {
      val q"$mods object $cName extends { ..$_ } with ..$parents { $_ => ..$body }" =
        originalCompanion.get
      // need to filter out Object, otherwise get duplicate Object error
      // also can't get a FQN of scala.AnyRef which gets erased to java.lang.Object, can't find a
      // sane way to =:= scala.AnyRef
      val filteredTraits =
        (traits ++ parents).toSet.filterNot(_.toString == "scala.AnyRef")
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
      q"override def toAvro: ($name => _root_.org.apache.avro.generic.GenericRecord) = ${p(c, SType)}.toAvro[$name]",
      q"override def fromTableRow: (${p(c, GModel)}.TableRow => $name) = ${p(c, SType)}.fromTableRow[$name]",
      q"override def toTableRow: ($name => ${p(c, GModel)}.TableRow) = ${p(c, SType)}.toTableRow[$name]"
    )
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
  private def shouldDumpClassesForPlugin =
    !BigQuerySysProps.DisableDump.value(default = "false").toBoolean

  private def getBQClassCacheDir: Path =
    // TODO: add this as key/value settings with default etc
    BigQuerySysProps.ClassCacheDirectory.valueOption.map(Paths.get(_)).getOrElse {
      Paths
        .get(CoreSysProps.TmpDir.value)
        .resolve(CoreSysProps.User.value)
        .resolve("generated-classes")
    }

  private def pShowCode(
    c: blackbox.Context
  )(records: Seq[c.Tree], caseClass: c.Tree): Seq[String] = {
    // print only records and case class and do it nicely so that we can just inject those
    // in scala plugin.
    import c.universe._
    (Seq(caseClass) ++ records).map {
      case q"$mods class $name[..$_] $_(..$fields) extends { ..$_ } with ..$parents { $_ => ..$_ }"
          if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        val f = fields
          .map { case ValDef(_, fname, ftpt, _) =>
            s"${SchemaUtil.escapeNameIfReserved(fname.toString)} : $ftpt"
          }
          .mkString(", ")
          .replaceAll(
            s"@${classOf[BigQueryTag].getName}",
            ""
          ) //BQ plugin does not need to know about BQTag
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

  private def genHashForMacro(owner: String, srcFile: String): String =
    Hashing
      .murmur3_32()
      .newHasher()
      .putString(owner, Charsets.UTF_8)
      .putString(srcFile, Charsets.UTF_8)
      .hash()
      .toString

  private def dumpCodeForScalaPlugin(
    c: blackbox.Context
  )(records: Seq[c.universe.Tree], caseClassTree: c.universe.Tree, name: String): Unit = {
    val owner = c.internal.enclosingOwner.fullName
    val srcFile = c.macroApplication.pos.source.file.canonicalPath
    val hash = genHashForMacro(owner, srcFile)

    val prettyCode = pShowCode(c)(records, caseClassTree).mkString("\n")
    val classCacheDir = getBQClassCacheDir
    val genSrcFile = classCacheDir.resolve(s"$name-$hash.scala").toFile

    logger.debug(s"Will dump generated $name of $owner from $srcFile to $genSrcFile")

    Files.createParentDirs(genSrcFile)
    Files.asCharSink(genSrcFile, Charsets.UTF_8).write(prettyCode)
  }
}
