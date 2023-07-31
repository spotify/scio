/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.parquet.tensorflow;

import com.google.protobuf.TextFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class ExampleReadSupport extends ReadSupport<Example> {

  public static String EXAMPLE_REQUESTED_PROJECTION = "parquet.example.projection";
  private static final String EXAMPLE_READ_SCHEMA = "parquet.example.read.schema";

  static final String EXAMPLE_SCHEMA_METADATA_KEY = "example.schema";
  static final String EXAMPLE_READ_SCHEMA_METADATA_KEY = "example.read.schema";

  /**
   * @param configuration a configuration
   * @param requestedProjection the requested projection schema
   * @see
   *     com.spotify.scio.parquet.tensorflow.ExampleParquetInputFormat#setRequestedProjection(org.apache.hadoop.mapreduce.Job,
   *     org.tensorflow.metadata.v0.Schema)
   */
  public static void setRequestedProjection(
      Configuration configuration, Schema requestedProjection) {
    configuration.set(
        EXAMPLE_REQUESTED_PROJECTION, TextFormat.printer().printToString(requestedProjection));
  }

  public static void setExampleReadSchema(Configuration configuration, Schema tfReadSchema) {
    configuration.set(EXAMPLE_READ_SCHEMA, TextFormat.printer().printToString(tfReadSchema));
  }

  @Override
  public ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    MessageType projection = fileSchema;
    Map<String, String> metadata = new LinkedHashMap<String, String>();

    String requestedProjectionString = configuration.get(EXAMPLE_REQUESTED_PROJECTION);
    if (requestedProjectionString != null) {
      try {
        Schema tfRequestedProjection = TextFormat.parse(requestedProjectionString, Schema.class);
        projection =
            new ExampleSchemaConverter(configuration)
                .convert(fileSchema.getName(), tfRequestedProjection);
      } catch (TextFormat.ParseException e) {
        throw new RuntimeException("Failre parsing projection schema", e);
      }
    }

    String tfReadSchema = configuration.get(EXAMPLE_READ_SCHEMA);
    if (tfReadSchema != null) {
      metadata.put(EXAMPLE_READ_SCHEMA_METADATA_KEY, tfReadSchema);
    }

    return new ReadContext(projection, metadata);
  }

  @Override
  public RecordMaterializer<Example> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    Map<String, String> metadata = readContext.getReadSupportMetadata();
    MessageType parquetSchema = readContext.getRequestedSchema();
    Schema tfSchema;
    try {
      if (metadata.get(EXAMPLE_READ_SCHEMA_METADATA_KEY) != null) {
        // use the example read schema provided by the user
        tfSchema = TextFormat.parse(metadata.get(EXAMPLE_READ_SCHEMA_METADATA_KEY), Schema.class);
      } else if (keyValueMetaData.get(EXAMPLE_SCHEMA_METADATA_KEY) != null) {
        // use the example schema from the file metadata if present
        tfSchema =
            TextFormat.parse(keyValueMetaData.get(EXAMPLE_SCHEMA_METADATA_KEY), Schema.class);
      } else {
        // default to converting the Parquet schema into an example schema
        tfSchema = new ExampleSchemaConverter(configuration).convert(parquetSchema);
      }
    } catch (TextFormat.ParseException e) {
      throw new RuntimeException("Invalid tensorflow schema", e);
    }

    return new ExampleRecordMaterializer(parquetSchema, tfSchema);
  }
}
