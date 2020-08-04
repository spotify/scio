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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of utilities for working with Avro files.
 *
 * <p>These utilities are based on the <a href="https://avro.apache.org/docs/1.8.1/spec.html">Avro
 * 1.8.1</a> specification.
 */
public class BigQueryAvroUtilsWrapper {

  /**
   * Defines the valid mapping between BigQuery types and native Avro types.
   *
   * <p>Some BigQuery types are duplicated here since slightly different Avro records are produced
   * when exporting data in Avro format and when reading data directly using the read API.
   */
  static final ImmutableMap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
      ImmutableMap.<String, Type>builder()
          .put("STRING", Type.STRING)
          .put("GEOGRAPHY", Type.STRING)
          .put("BYTES", Type.BYTES)
          .put("INTEGER", Type.LONG)
          .put("FLOAT", Type.DOUBLE)
          .put("NUMERIC", Type.BYTES)
          .put("BOOLEAN", Type.BOOLEAN)
          .put("TIMESTAMP", Type.LONG)
          .put("RECORD", Type.RECORD)
          .put("DATE", Type.INT)
          .put("DATETIME", Type.STRING)
          .put("TIME", Type.LONG)
          .build();

  public static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
    return BigQueryAvroUtils.convertGenericRecordToTableRow(record, schema);
  }

  public static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
    List<Field> avroFields = new ArrayList<>();
    for (TableFieldSchema bigQueryField : fieldSchemas) {
      avroFields.add(convertField(bigQueryField));
    }
    return Schema.createRecord(
        schemaName,
        "Translated Avro Schema for " + schemaName,
        "org.apache.beam.sdk.io.gcp.bigquery",
        false,
        avroFields);
  }

  private static Field convertField(TableFieldSchema bigQueryField) {
    Type avroType = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
    if (avroType == null) {
      throw new IllegalArgumentException(
          "Unable to map BigQuery field type " + bigQueryField.getType() + " to avro type.");
    }

    Schema elementSchema;
    if (avroType == Type.RECORD) {
      elementSchema = toGenericAvroSchema(bigQueryField.getName(), bigQueryField.getFields());
    } else {
      elementSchema = Schema.create(avroType);
      if (bigQueryField.getType().equals("DATE")) {
        elementSchema = LogicalTypes.date().addToSchema(elementSchema);
      }
      if (bigQueryField.getType().equals("TIME")) {
        elementSchema = LogicalTypes.timeMicros().addToSchema(elementSchema);
      }
      if (bigQueryField.getType().equals("DATETIME") && avroType != Type.STRING) {
        throw new IllegalArgumentException("DateTime type is not supported");
      }
    }
    Schema fieldSchema;
    if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
      fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
    } else if ("REQUIRED".equals(bigQueryField.getMode())) {
      fieldSchema = elementSchema;
    } else if ("REPEATED".equals(bigQueryField.getMode())) {
      fieldSchema = Schema.createArray(elementSchema);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
    }
    return new Field(
        bigQueryField.getName(),
        fieldSchema,
        bigQueryField.getDescription(),
        (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
  }
}
