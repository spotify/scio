package com.spotify.scio.bigquery.types

import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery.types.MacroUtil._
import com.spotify.scio.bigquery.{BigQueryClient, Util}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.macros._

private[types] object TypeProvider {

  private lazy val bigquery: BigQueryClient = BigQueryClient()

  def tableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val tableSpec = extractString(c, "Missing table specification")
    val schema = bigquery.getTableSchema(tableSpec)
    val traits = List(tq"${p(c, SType)}.HasTable")
    val overrides = List(q"override def table: ${p(c, GModel)}.TableReference = ${p(c, SUtil)}.parseTableSpec($tableSpec)")

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  def schemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val schemaString = extractString(c, "Missing schema")
    val schema = Util.parseSchema(schemaString)
    schemaToType(c)(schema, annottees, Nil, Nil)
  }

  def queryImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val query = extractString(c, "Missing query")
    val schema = bigquery.getQuerySchema(query)
    val traits = Seq(tq"${p(c, SType)}.HasQuery")
    val overrides = Seq(q"override def query: _root_.java.lang.String = $query")

    schemaToType(c)(schema, annottees, traits, overrides)
  }

  def toTableImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val r = annottees.map(_.tree) match {
      case List(q"case class $name(..$fields) { ..$body }") =>
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SType)}.schemaOf[$name]"
        q"""${caseClass(c)(name, fields, body)}
            ${companion(c)(name, Nil, Seq(defSchema), fields.asInstanceOf[Seq[Tree]].size)}
        """
      case _ => c.abort(c.enclosingPosition, "Invalid annotation")
    }
    debug(s"TypeProvider.toTableImpl:")
    debug(r)
    c.Expr[Any](r)
  }

  private def schemaToType(c: blackbox.Context)
                          (schema: TableSchema, annottees: Seq[c.Expr[Any]],
                           traits: Seq[c.Tree], overrides: Seq[c.Tree]): c.Expr[Any] = {
    import c.universe._

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
        (q"${Ident(TypeName(name))}", Seq(q"case class ${TypeName(name)}(..$fields)") ++ records)
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
      case List(q"class $name") =>
        val defSchema = q"override def schema: ${p(c, GModel)}.TableSchema = ${p(c, SUtil)}.parseSchema(${schema.toString})"
        q"""${caseClass(c)(name, fields, Nil)}
            ${companion(c)(name, traits, Seq(defSchema) ++ overrides, fields.size)}
            ..$records
        """
      case _ => c.abort(c.enclosingPosition, "Invalid annotation")
    }
    debug(s"TypeProvider.schemaToType[$schema]:")
    debug(r)
    c.Expr[Any](r)
  }

  /** Extract string from annotation. */
  private def extractString(c: blackbox.Context, errorMessage: String): String = {
    import c.universe._

    c.macroApplication match {
      // @annotation("string literal")
      case Apply(Select(Apply(_, List(Literal(Constant(s: String)))), _), _) => s
      // @annotation("string literal".stripMargin)
      case Apply(Select(Apply(_, List(Select(Literal(Constant(s: String)), TermName("stripMargin")))), _), _) =>
        s.stripMargin
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }
  }

  /** Generate a case class. */
  private def caseClass(c: blackbox.Context)
                       (name: c.TypeName, fields: Seq[c.Tree], body: Seq[c.Tree]): c.Tree = {
    import c.universe._
    q"case class $name(..$fields) extends ${p(c, SType)}.HasAnnotation { ..$body }"
  }
  /** Generate a companion object. */
  private def companion(c: blackbox.Context)
                       (name: c.TypeName, traits: Seq[c.Tree], methods: Seq[c.Tree], numFields: Int): c.Tree = {
    import c.universe._
    val tupled = if (numFields > 1) Seq(q"def tupled = (${TermName(name.toString)}.apply _).tupled") else Nil
    val m = converters(c)(name) ++ tupled ++ methods
    q"""object ${TermName(name.toString)} extends ${p(c, SType)}.HasSchema[$name] with ..$traits {
          ..$m
        }
    """
  }
  /** Generate override converter methods for HasSchema[T]. */
  private def converters(c: blackbox.Context)(name: c.TypeName): Seq[c.Tree] = {
    import c.universe._
    List(
      q"override def fromTableRow: (${p(c, GModel)}.TableRow => $name) = ${p(c, SType)}.fromTableRow[$name]",
      q"override def toTableRow: ($name => ${p(c, GModel)}.TableRow) = ${p(c, SType)}.toTableRow[$name]")
  }

}

private[types] object NameProvider {

  private val m = MMap.empty[String, Int]

  /**
   * Generate a unique name for a nested record.
   * This is necessary since we create case classes for nested records and name them with their
   * field names.
   */
  def getUniqueName(name: String): String = m.synchronized {
    val cName = camelCase(name) + '$'
    if (m.contains(cName)) {
      m(cName) += 1
      cName + m(cName)
    } else {
      m.put(cName, 1)
      cName
    }
  }

  private def camelCase(s: String): String =
    s.split('_').filter(_.nonEmpty).map(t => t(0).toUpper + t.drop(1).toLowerCase).mkString("")
}
