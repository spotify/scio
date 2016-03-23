/*
 * Copyright 2016 Spotify AB.
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

package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Sink for Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for writing to HDFS.
 */
public class SimpleAuthHadoopFileSink<K, V> extends HadoopFileSink<K, V>  {
  private final String user;

  public SimpleAuthHadoopFileSink(String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  String user) {
    super(path, formatClass);
    this.user = user;
  }

  public SimpleAuthHadoopFileSink(String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  String user,
                                  Configuration conf) {
    super(path, formatClass, conf);
    this.user = user;
  }

  @Override
  public void validate(PipelineOptions options) {
    HadoopUserUtils.setSimpleAuthUser(user);
    super.validate(options);
  }

  @Override
  public WriteOperation<KV<K, V>, ?> createWriteOperation(PipelineOptions options) {
    return new SimpleAuthHadoopWriteOperation<>(this, path, formatClass, user);
  }


  public static class SimpleAuthHadoopWriteOperation<K, V> extends HadoopFileSink.HadoopWriteOperation<K, V> {
    private final String user;

    public SimpleAuthHadoopWriteOperation(Sink<KV<K, V>> sink,
                                   String path, Class<? extends FileOutputFormat<K, V>> formatClass,
                                   String user) {
      super(sink, path, formatClass);
      this.user = user;
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {
      HadoopUserUtils.setSimpleAuthUser(user);
      super.initialize(options);
    }

    @Override
    public void finalize(Iterable<String> writerResults, PipelineOptions options) throws Exception {
      HadoopUserUtils.setSimpleAuthUser(user);
      super.finalize(writerResults, options);
    }

    @Override
    public Writer<KV<K, V>, String> createWriter(PipelineOptions options) throws Exception {
      return new SimpleAuthHadoopWriter<>(this, path, formatClass, user);
    }
  }

  public static class SimpleAuthHadoopWriter<K, V> extends HadoopFileSink.HadoopWriter<K, V> {
    private final String user;

    public SimpleAuthHadoopWriter(HadoopWriteOperation<K, V> writeOperation,
                           String path,
                           Class<? extends FileOutputFormat<K, V>> formatClass,
                           String user) {
      super(writeOperation, path, formatClass);
      this.user = user;
    }

    @Override
    public void open(String uId) throws Exception {
      HadoopUserUtils.setSimpleAuthUser(user);
      super.open(uId);
    }
  }
}
