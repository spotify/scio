package com.spotify.scio.sql
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas.{Record, ScalarWrapper, Schema, SchemaMaterializer}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.values.{PCollectionTuple, Row, TupleTag}
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import com.spotify.scio.sql.syntax.sqltransform._
import org.apache.beam.sdk.extensions.sql.impl.ParseException

import scala.language.experimental.macros

object Sql {

  def from[A: Schema](sc: SCollection[A]) = SqlSCollection(sc)

  def from[A: Schema, B: Schema](a: (String, SCollection[A]), b: (String, SCollection[B])) =
    SqlSCollection2(a, b)

}

final case class SqlSCollection[A: Schema](sc: SCollection[A]) {

  def query(query: String, udfs: Udf*): SCollection[Row] = {
    sc.context.wrap {
      val sqlTransform = SqlTransform.query(query).registerUdf(udfs: _*)
      QueryUtils.transform(sc).applyInternal(sqlTransform)
    }
  }

  def queryAs[C: Schema](q: String, udfs: Udf*): SCollection[C] =
    try {
      val (schema, to, from) = SchemaMaterializer.materialize(sc.context, Schema[C])
      query(q, udfs: _*).map[C](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    } catch {
      case e: ParseException =>
        val schema: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
        val expectedSchema: BSchema =
          Schema[C] match {
            case s: Record[C] =>
              SchemaMaterializer.fieldType(s).getRowSchema
            case _ =>
              SchemaMaterializer.fieldType(Schema[ScalarWrapper[C]]).getRowSchema
          }

        QueryUtils
          .typecheck(q, schema, expectedSchema)
          .fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

  def typecheck[C: Schema](q: String, udfs: Udf*): Either[String, SCollection[C]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val expectedSchema: BSchema =
      Schema[C] match {
        case s: Record[C] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[C]]).getRowSchema
      }

    QueryUtils.typecheck(q, schema, expectedSchema).right.map { _ =>
      val (schema, to, from) = SchemaMaterializer.materialize(sc.context, Schema[C])
      query(q, udfs: _*)
        .map[C](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    }
  }

}

final case class SqlSCollection2[A: Schema, B: Schema](a: (String, SCollection[A]),
                                                       b: (String, SCollection[B])) {

  def query(q: String, udfs: Udf*): SCollection[Row] = {
    a._2.context.wrap {
      val collA = QueryUtils.transform(a._2)
      val collB = QueryUtils.transform(b._2)
      val sqlTransform = SqlTransform.query(q).registerUdf(udfs: _*)

      PCollectionTuple
        .of(new TupleTag[A](a._1), collA.internal)
        .and(new TupleTag[B](b._1), collB.internal)
        .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)
    }
  }

  def queryAs[C: Schema](q: String, udfs: Udf*): SCollection[C] =
    typecheck[C](q, udfs: _*).fold(err => throw new RuntimeException(err), identity)

  def typecheck[C: Schema](q: String, udfs: Udf*): Either[String, SCollection[C]] = {
    val schemaA: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val schemaB: BSchema = SchemaMaterializer.fieldType(Schema[B]).getRowSchema
    val expectedSchema: BSchema =
      Schema[C] match {
        case s: Record[C] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[C]]).getRowSchema
      }

    QueryUtils.typecheck(q, List((a._1, schemaA), (b._1, schemaB)), expectedSchema).right.map { _ =>
      val (schema, to, from) = SchemaMaterializer.materialize(a._2.context, Schema[C])
      query(q, udfs: _*)
        .map[C](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    }
  }

}
