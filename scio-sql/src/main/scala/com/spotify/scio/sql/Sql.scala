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

package com.spotify.scio.sql

import java.util.Collections

import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils
import org.apache.beam.sdk.extensions.sql.impl.{BeamSqlEnv, BeamTableStatistics}
import org.apache.beam.sdk.extensions.sql.meta.provider.{ReadOnlyTableProvider, TableProvider}
import org.apache.beam.sdk.extensions.sql.meta.{BaseBeamTable, BeamSqlTable}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import org.apache.beam.sdk.values._
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object Sql extends SqlSCollections {
  private[sql] val BeamProviderName = "beam"
  private[sql] val SCollectionTypeName = "SCOLLECTION"

  private[scio] def defaultTag[A]: TupleTag[A] = new TupleTag[A](SCollectionTypeName)

  private[sql] def registerUdf(t: SqlTransform, udfs: Udf*): SqlTransform =
    udfs.foldLeft(t) {
      case (st, x: UdfFromClass[_]) =>
        st.registerUdf(x.fnName, x.clazz)
      case (st, x: UdfFromSerializableFn[_, _]) =>
        st.registerUdf(x.fnName, x.fn)
      case (st, x: UdafFromCombineFn[_, _, _]) =>
        st.registerUdaf(x.fnName, x.fn)
    }

  private[sql] def tableProvider[A](tag: TupleTag[A], sc: SCollection[A]): TableProvider = {
    val table = new BeamPCollectionTable[A](sc.internal)
    new ReadOnlyTableProvider(SCollectionTypeName, Collections.singletonMap(tag.getId, table))
  }

  private[sql] def setSchema[T: Schema: ClassTag](c: SCollection[T]): SCollection[T] =
    c.transform { x =>
      val (schema, to, from) = SchemaMaterializer.materialize(Schema[T])
      val td = TypeDescriptor.of(ScioUtil.classOf[T])
      x.map(identity)(Coder.beam(SchemaCoder.of(schema, td, to, from)))
    }
}

private object Queries {
  private[this] val TableNotFound = raw"(SqlValidatorException: Table '(.*)' not found)".r
  private[this] val UnknownIdentifier = raw"(SqlValidatorException: Unknown identifier '(.*)')".r

  private[this] def parseQuery(
    query: String,
    schemas: List[(String, BSchema)],
    udfs: List[Udf]
  ): Try[BeamRelNode] = Try {
    val tables: Map[String, BeamSqlTable] = schemas.map { case (tag, schema) =>
      tag -> new BaseBeamTable {
        override def buildIOReader(begin: PBegin): PCollection[Row] = ???

        override def buildIOWriter(input: PCollection[Row]): POutput = ???

        override def isBounded: PCollection.IsBounded = PCollection.IsBounded.BOUNDED

        override def getTableStatistics(options: PipelineOptions): BeamTableStatistics =
          BeamTableStatistics.BOUNDED_UNKNOWN

        override def getSchema: BSchema = schema
      }
    }.toMap

    val tableProvider = new ReadOnlyTableProvider(Sql.SCollectionTypeName, tables.asJava)
    val env = BeamSqlEnv.builder(tableProvider).setPipelineOptions(PipelineOptionsFactory.create())
    udfs.foreach {
      case x: UdfFromClass[_] =>
        env.addUdf(x.fnName, x.clazz)
      case x: UdfFromSerializableFn[_, _] =>
        env.addUdf(x.fnName, x.fn)
      case x: UdafFromCombineFn[_, _, _] =>
        env.addUdaf(x.fnName, x.fn)
    }
    env.build().parseQuery(query)
  }

  private[this] def schema(
    query: String,
    schemas: List[(String, BSchema)],
    udfs: List[Udf]
  ): Try[BSchema] =
    parseQuery(query, schemas, udfs).map(n => CalciteUtils.toSchema(n.getRowType))

  private[this] def printInferred(inferredSchemas: List[(String, BSchema)]): String =
    inferredSchemas
      .map {
        case (name, null) =>
          s"could not infer schema for $name"
        case (name, schema) =>
          s"""
          |schema of $name:
          |${PrettyPrint.prettyPrint(schema.getFields.asScala.toList)}
        """.stripMargin
      }
      .mkString("\n")

  def typecheck(
    query: String,
    inferredSchemas: List[(String, BSchema)],
    expectedSchema: BSchema,
    udfs: List[Udf]
  ): Either[String, String] =
    schema(query, inferredSchemas, udfs).toEither.left
      .map { ex =>
        val mess = ExceptionUtils.getRootCauseMessage(ex) match {
          case TableNotFound(msg, _) =>
            s"$msg\nHint: incorrect table aliases are being used!"
          case UnknownIdentifier(msg, _) =>
            s"$msg\nHint: incorrect table aliases are being used!"
          case msg =>
            msg
        }

        s"""
           |$mess
           |
           |Query:
           |$query
           |
           |${printInferred(inferredSchemas)}
           |Query result schema (inferred) is unknown.
           |Expected schema:
           |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
      }
      .right
      .flatMap {
        case inferredSchema
            if SchemaTypes.equal(
              BSchema.FieldType.row(inferredSchema),
              BSchema.FieldType.row(expectedSchema)
            ) =>
          Right(query)
        case inferredSchema =>
          val message =
            s"""
               |Inferred schema for query is not compatible with the expected schema.
               |
               |Query:
               |$query
               |
               |${printInferred(inferredSchemas)}
               |Query result schema (inferred):
               |${PrettyPrint.prettyPrint(inferredSchema.getFields.asScala.toList)}
               |
               |Expected schema:
               |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
}
