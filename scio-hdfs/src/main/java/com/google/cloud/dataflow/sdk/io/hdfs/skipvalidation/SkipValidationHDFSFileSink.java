/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.sdk.io.hdfs.skipvalidation;

import com.google.cloud.dataflow.sdk.io.hdfs.HDFSFileSink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A {@code Sink} for writing records to a Hadoop filesystem using a Hadoop file-based output
 * format with Simple Authentication.
 *
 * Skips validation for hdfs location (in case the permissions don't exist to read it on the
 * calling machine)
 *
 * @param <K> The type of keys to be written to the sink.
 * @param <V> The type of values to be written to the sink.
 */
public class SkipValidationHDFSFileSink<K, V> extends HDFSFileSink<K, V> {

  public SkipValidationHDFSFileSink(String path,
                                Class<? extends FileOutputFormat<K, V>> formatClass,
                                Configuration conf) {
    super(path, formatClass, conf);
  }

  @Override
  public void validate(PipelineOptions options) {
  }
}
