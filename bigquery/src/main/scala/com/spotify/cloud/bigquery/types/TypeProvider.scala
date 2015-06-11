package com.spotify.cloud.bigquery.types

import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.cloud.bigquery.types.MacroUtil._
import com.spotify.cloud.bigquery.{BigQueryClient, Util}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.macros._

private[types] object TypeProvider {

  private lazy val bigquery: BigQueryClient = BigQueryClient()

  private def extractString(c: blackbox.Context, errorMessage: String): String = {
    import c.universe._

    c.macroApplication match {
      case Apply(Select(Apply(_, List(Literal(Constant(s: String)))), _), _) => s
      case Apply(Select(Apply(_, List(Select(Literal(Constant(s: String)), TermName("stripMargin")))), _), _) =>
        s.stripMargin
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }
  }

  def tableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val tableSpec = extractString(c, "Missing table specification")
    val schema = bigquery.getTableSchema(tableSpec)

    val extraTrait = tq"${p(c, SBQT)}.HasTable"
    val extraOverrides =
      List(q"override def table: ${p(c, GBQM)}.TableReference = ${p(c, SBQ)}.Util.parseTableSpec($tableSpec)")

    schemaToType(c)(schema, annottees, extraTrait, extraOverrides)
  }

  def schemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val schemaString = extractString(c, "Missing schema")
    val schema = Util.parseSchema(schemaString)

    schemaToType(c)(schema, annottees, null, Nil)
  }

  def queryImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val query = extractString(c, "Missing query")
    val schema = bigquery.getQuerySchema(query)

    val extraTrait = tq"${p(c, SBQT)}.HasQuery"
    val extraOverrides = List(q"override def query: _root_.java.lang.String = $query")

    schemaToType(c)(schema, annottees, extraTrait, extraOverrides)
  }

  def toTableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val r = annottees.map(_.tree) match {
      case List(q"case class $name(..$fields) { ..$body }") =>
        val ts = tq"${p(c, GBQM)}.TableSchema"
        val caseClass = q"case class $name(..$fields) { ..$body }"
        val methods = List(
          q"override def schema: $ts = ${p(c, SBQT)}.schemaOf[$name]",
          q"def fromTableRow = ${p(c, SBQT)}.fromTableRow[$name]",
          q"def toTableRow = ${p(c, SBQT)}.toTableRow[$name]"
        )
        val companion = q"object ${TermName(name.toString)} extends ${p(c, SBQT)}.HasSchema { ..$methods }"

        q"""$caseClass
            $companion
        """
      case _ => c.abort(c.enclosingPosition, "Invalid annotation")
    }
    debug(s"TypeProvider.toTableImpl:")
    debug(r)
    c.Expr[Any](r)
  }

  private def schemaToType(c: blackbox.Context)
                          (schema: TableSchema, annottees: Seq[c.Expr[Any]],
                           extraTrait: c.Tree , extraOverrides: List[c.Tree]): c.Expr[Any] = {
    import c.universe._

    // Returns: (raw type, e.g. Int, String, NestedRecord, nested case class definitions)
    def getRawType(tfs: TableFieldSchema): (Tree, Seq[Tree]) = tfs.getType match {
      case "INTEGER" => (tq"_root_.scala.Long", Seq())
      case "FLOAT" => (tq"_root_.scala.Double", Seq())
      case "BOOLEAN" => (tq"_root_.scala.Boolean", Seq())
      case "STRING" => (tq"_root_.java.lang.String", Seq())
      case "TIMESTAMP" => (tq"_root_.org.joda.time.Instant", Seq())
      case "RECORD" => {
        val name = NameProvider.getUniqueName(tfs.getName)
        val (fields, records) = toFields(tfs.getFields)
        (q"${Ident(TypeName(name))}", Seq(q"case class ${TypeName(name)}(..$fields)") ++ records)
      }
      case t => c.abort(c.enclosingPosition, s"type: $t not supported")
    }

    // Returns: (field type, e.g. T/Option[T]/List[T], nested case class definitions)
    def getFieldType(tfs: TableFieldSchema): (Tree, Seq[Tree]) = {
      val (t, r) = getRawType(tfs)
      val ft = tfs.getMode match {
        case "NULLABLE" => tq"_root_.scala.Option[$t]"
        case "REQUIRED" => t
        case "REPEATED" => tq"_root_.scala.List[$t]"
        case m => c.abort(c.enclosingPosition, s"mode: $m not supported")
      }
      (ft, r)
    }

    // Returns: ("fieldName: fieldType", nested case class definitions)
    def toField(tfs: TableFieldSchema): (Tree, Seq[Tree]) = {
      val (ft, r) = getFieldType(tfs)
      (q"${TermName(tfs.getName)}: $ft", r)
    }

    def toFields(fields: JList[TableFieldSchema]): (Seq[Tree], Seq[Tree]) = {
      val f = fields.asScala.map(s => toField(s))
      (f.map(_._1), f.flatMap(_._2))
    }

    val (fields, records) = toFields(schema.getFields)

    val r = annottees.map(_.tree) match {
      case List(q"class $name") => {
        val ts = tq"${p(c, GBQM)}.TableSchema"
        val caseClass = q"case class $name(..$fields)"
        val methods = List(
          q"override def schema: $ts = ${p(c, SBQ)}.Util.parseSchema(${schema.toString})",
          q"def fromTableRow = ${p(c, SBQT)}.fromTableRow[$name]",
          q"def toTableRow = ${p(c, SBQT)}.toTableRow[$name]"
        ) ++ extraOverrides
        val companion = if (extraTrait != null) {
          q"object ${TermName(name.toString)} extends ${p(c, SBQT)}.HasSchema with $extraTrait { ..$methods }"
        } else {
          q"object ${TermName(name.toString)} extends ${p(c, SBQT)}.HasSchema { ..$methods }"
        }

        q"""$caseClass
            $companion
            ..$records
        """
      }
      case _ => c.abort(c.enclosingPosition, "Invalid annotation")
    }
    debug(s"TypeProvider.schemaToType[$schema]:")
    debug(r)
    c.Expr[Any](r)
  }

}

private[types] object NameProvider {

  private val m = MMap.empty[String, Int]

  def getUniqueName(name: String): String = m.synchronized {
    if (m.contains(name)) {
      m(name) += 1
      name + m(name)
    } else {
      m.put(name, 1)
      name
    }
  }

}
