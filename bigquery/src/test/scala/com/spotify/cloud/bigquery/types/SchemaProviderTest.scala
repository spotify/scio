package com.spotify.cloud.bigquery.types

import java.io.StringReader

import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableSchema
import org.scalatest.{Matchers, FlatSpec}

class SchemaProviderTest extends FlatSpec with Matchers {

  import Schemas._

  def tableSchema(schemaString: String): TableSchema =
    new JsonObjectParser(new JacksonFactory).parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  def basicFields(mode: String) =
    s"""
       |"fields": [
       |  {"mode": "$mode", "name": "f1", "type": "INTEGER"},
       |  {"mode": "$mode", "name": "f2", "type": "INTEGER"},
       |  {"mode": "$mode", "name": "f3", "type": "FLOAT"},
       |  {"mode": "$mode", "name": "f4", "type": "FLOAT"},
       |  {"mode": "$mode", "name": "f5", "type": "BOOLEAN"},
       |  {"mode": "$mode", "name": "f6", "type": "STRING"},
       |  {"mode": "$mode", "name": "f7", "type": "TIMESTAMP"}
       |]
       |""".stripMargin

  "SchemaProvider.toSchema" should "support required primitive types" in {
    SchemaProvider.schemaOf[P1] should equal (tableSchema(s"{${basicFields("REQUIRED")}}"))
  }

  it should "support nullable primitive types" in {
    SchemaProvider.schemaOf[P2] should equal (tableSchema(s"{${basicFields("NULLABLE")}}"))
  }

  it should "support repeated primitive types" in {
    SchemaProvider.schemaOf[P3] should equal (tableSchema(s"{${basicFields("REPEATED")}}"))
  }

  def recordFields(mode: String) =
    s"""
       |{
       |  "fields": [
       |    {"mode": "$mode", "name": "f1", "type": "RECORD", ${basicFields("REQUIRED")}},
       |    {"mode": "$mode", "name": "f2", "type": "RECORD", ${basicFields("NULLABLE")}},
       |    {"mode": "$mode", "name": "f3", "type": "RECORD", ${basicFields("REPEATED")}}
       |  ]
       |}
       |""".stripMargin

  it should "support required records" in {
    SchemaProvider.schemaOf[R1] should equal (tableSchema(recordFields("REQUIRED")))
  }

  it should "support nullable records" in {
    SchemaProvider.schemaOf[R2] should equal (tableSchema(recordFields("NULLABLE")))
  }

  it should "support repeated records" in {
    SchemaProvider.schemaOf[R3] should equal (tableSchema(recordFields("REPEATED")))
  }

}
