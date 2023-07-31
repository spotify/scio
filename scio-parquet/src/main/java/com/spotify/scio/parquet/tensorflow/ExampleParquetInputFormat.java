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

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class ExampleParquetInputFormat extends ParquetInputFormat<Example> {

  /**
   * Set the subset of columns to read (projection pushdown). Specified as an tensorflow schema, the
   * requested projection is converted into a Parquet schema for Parquet column projection.
   *
   * <p>This is useful if the full schema is large and you only want to read a few columns, since it
   * saves time by not reading unused columns.
   *
   * <p>If a requested projection is set, then the tensorflow schema used for reading must be
   * compatible with the projection. Use {@link
   * #setExampleReadSchema(org.apache.hadoop.mapreduce.Job, org.tensorflow.metadata.v0.Schema)} to
   * set a read schema, if needed.
   *
   * @param job a job
   * @param requestedProjection the requested projection schema
   * @see #setAvroReadSchema(org.apache.hadoop.mapreduce.Job, org.tensorflow.metadata.v0.Schema)
   * @see
   *     com.spotify.scio.parquet.tensorflow.ExampleParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job,
   *     org.tensorflow.metadata.v0.Schema)
   */
  public static void setRequestedProjection(Job job, Schema requestedProjection) {
    ExampleReadSupport.setRequestedProjection(
        ContextUtil.getConfiguration(job), requestedProjection);
  }

  /**
   * Override the tensorflow schema to use for reading. If not set, the tensorflow schema used for
   * writing is used.
   *
   * @param job a job
   * @param tfReadSchema the requested schema
   * @see #setRequestedProjection(org.apache.hadoop.mapreduce.Job,
   *     org.tensorflow.metadata.v0.Schema)
   * @see
   *     com.spotify.scio.parquet.tensorflow.ExampleParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job,
   *     org.tensorflow.metadata.v0.Schema)
   */
  public static void setExampleReadSchema(Job job, Schema tfReadSchema) {
    ExampleReadSupport.setExampleReadSchema(ContextUtil.getConfiguration(job), tfReadSchema);
  }
}
