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

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class ExampleRecordMaterializer extends RecordMaterializer<Example> {

  private ExampleConverters.ExampleConverter root;

  public ExampleRecordMaterializer(MessageType requestedSchema, Schema tfSchema) {
    this.root = new ExampleConverters.ExampleConverter(requestedSchema, tfSchema);
  }

  @Override
  public Example getCurrentRecord() {
    return root.get();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
