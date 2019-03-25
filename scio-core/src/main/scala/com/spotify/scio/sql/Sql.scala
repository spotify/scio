package com.spotify.scio.sql
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.{BeamSqlTable, SqlTransform}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import com.spotify.scio.sql.syntax.sqltransform._
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.beam.sdk.extensions.sql.impl.{BeamSqlEnv, ParseException}
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils

import scala.language.experimental.macros
import scala.util.Try
import scala.collection.JavaConverters._

final case class Query[A, B](query: String, udfs: List[Udf] = Nil)

final case class Query2[A, B, C](query: String,
                                 udfs: List[Udf] = Nil,
                                 aTag: TupleTag[A],
                                 bTag: TupleTag[B])

object Queries {
  def typecheck[A: Schema, B: Schema](q: Query[A, B]): Either[String, Query[A, B]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val expectedSchema: BSchema =
      Schema[B] match {
        case s: Record[B] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[B]]).getRowSchema
      }

    QueryUtils.typecheck(q.query, schema, expectedSchema).right.map(_ => q)
  }

  def typecheck[A: Schema, B: Schema, C: Schema](
    q: Query2[A, B, C]): Either[String, Query2[A, B, C]] = {
    val schemaA: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val schemaB: BSchema = SchemaMaterializer.fieldType(Schema[B]).getRowSchema
    val expectedSchema: BSchema =
      Schema[C] match {
        case s: Record[C] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[C]]).getRowSchema
      }

    QueryUtils
      .typecheck(q.query, List((q.aTag.getId, schemaA), (q.bTag.getId, schemaB)), expectedSchema)
      .right
      .map(_ => q)
  }
}

object Sql {

  def from[A: Schema](sc: SCollection[A]) = SqlSCollection(sc)

  def from[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]) =
    SqlSCollection2(a, b)

}

final case class SqlSCollection[A: Schema](sc: SCollection[A]) {

  def queryRaw(q: String, udfs: List[Udf] = Nil): SCollection[Row] = query(Query(q, udfs))

  def query(q: Query[A, Row]): SCollection[Row] = {
    sc.context.wrap {
      val sqlTransform = SqlTransform.query(q.query).registerUdf(q.udfs: _*)
      QueryUtils.transform(sc).applyInternal(sqlTransform)
    }
  }

  def queryRawAs[C: Schema](q: String, udfs: List[Udf] = Nil): SCollection[C] =
    queryAs(Query(q, udfs))

  def queryAs[C: Schema](q: Query[A, C]): SCollection[C] =
    try {
      val (schema, to, from) = SchemaMaterializer.materialize(sc.context, Schema[C])
      queryRaw(q.query, q.udfs).map[C](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class SqlSCollection2[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]) {

  def queryRaw(q: String,
               aTag: TupleTag[A],
               bTag: TupleTag[B],
               udfs: List[Udf] = Nil): SCollection[Row] =
    query(Query2(q, udfs, aTag, bTag))

  def query(q: Query2[A, B, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = QueryUtils.transform(a)
      val collB = QueryUtils.transform(b)
      val sqlTransform = SqlTransform.query(q.query).registerUdf(q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)
    }
  }

  def queryRawAs[C: Schema](q: String,
                            aTag: TupleTag[A],
                            bTag: TupleTag[B],
                            udfs: List[Udf] = Nil): SCollection[C] =
    queryAs(Query2(q, udfs, aTag, bTag))

  def queryAs[C: Schema](q: Query2[A, B, C]): SCollection[C] =
    try {
      val (schema, to, from) = SchemaMaterializer.materialize(a.context, Schema[C])
      queryRaw(q.query, q.aTag, q.bTag, q.udfs)
        .map[C](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

object QueryUtils {

  private[this] val PCollectionName = "PCOLLECTION"

  def transform[T: Schema](c: SCollection[T]): SCollection[T] = {
    val coderT: Coder[T] = {
      val (schema, to, from) = SchemaMaterializer.materialize(c.context, Schema[T])
      Coder.beam(SchemaCoder.of(schema, to, from))
    }
    c.transform(s"${c.tfName}: set schema")(_.map(identity)(coderT))
  }

  def parseQuery(query: String, schemas: (String, BSchema)*): Try[BeamRelNode] = Try {
    val tables: Map[String, BeamSqlTable] = schemas.map {
      case (tag, schema) =>
        tag -> new BaseBeamTable(schema) {
          override def buildIOReader(begin: PBegin): PCollection[Row] = ???

          override def buildIOWriter(input: PCollection[Row]): POutput = ???

          override def isBounded: PCollection.IsBounded = PCollection.IsBounded.BOUNDED
        }
    }.toMap

    BeamSqlEnv.readOnly(PCollectionName, tables.asJava).parseQuery(query)
  }

  def parseQuery(query: String, schema: BSchema): Try[BeamRelNode] =
    parseQuery(query, (PCollectionName, schema))

  def schema(query: String, schemas: (String, BSchema)*): Try[BSchema] =
    parseQuery(query, schemas: _*).map(n => CalciteUtils.toSchema(n.getRowType))

  def schema(query: String, s: BSchema): Try[BSchema] = schema(query, (PCollectionName, s))

  def typecheck(query: String,
                inferredSchema: BSchema,
                expectedSchema: BSchema): Either[String, String] =
    typecheck(query, List((PCollectionName, inferredSchema)), expectedSchema)

  def typecheck(query: String,
                inferredSchemas: List[(String, BSchema)],
                expectedSchema: BSchema): Either[String, String] = {
    ScioUtil
      .toEither(schema(query, inferredSchemas: _*))
      .left
      .map { ex =>
        val mess = org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage(ex)
        s"""
           |$mess
           |
           |Query:
           |$query
           |
           |PCOLLECTION schema:
           |${inferredSchemas.map(i => PrettyPrint.prettyPrint(i._2.getFields.asScala.toList))}
           |Query result schema (infered) is unknown
           |Expected schema:
           |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
      }
      .right
      .flatMap {
        case inferredSchema
            if SchemaTypes.typesEqual(BSchema.FieldType.row(inferredSchema),
                                      BSchema.FieldType.row(expectedSchema)) =>
          Right(query)
        case inferredSchema =>
          val message =
            s"""
               |Infered schema for query is not compatible with the expected schema.
               |
               |Query:
               |$query
               |
               |PCOLLECTION schema:
               |${inferredSchemas.map(i => PrettyPrint.prettyPrint(i._2.getFields.asScala.toList))}
               |Query result schema (infered):
               |${PrettyPrint.prettyPrint(inferredSchema.getFields.asScala.toList)}
               |Expected schema:
               |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
  }
}
