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

package com.spotify.parquet.tensorflow;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class TensorflowExampleParquetOutputFormat extends ParquetOutputFormat<Example> {

  /**
   * Set the example schema to use for writing. The schema is translated into a Parquet schema so
   * that the records can be written in Parquet format. It is also stored in the Parquet metadata so
   * that records can be reconstructed as example objects at read time without specifying a read
   * schema.
   *
   * @param job a job
   * @param schema a schema for the data that will be written
   * @see TensorflowExampleParquetInputFormat#setExampleReadSchema(org.apache.hadoop.mapreduce.Job,
   *     org.tensorflow.metadata.v0.Schema)
   */
  public static void setSchema(Job job, Schema schema) {
    TensorflowExampleWriteSupport.setSchema(ContextUtil.getConfiguration(job), schema);
  }

  public TensorflowExampleParquetOutputFormat() {
    super(new TensorflowExampleWriteSupport());
  }
}
