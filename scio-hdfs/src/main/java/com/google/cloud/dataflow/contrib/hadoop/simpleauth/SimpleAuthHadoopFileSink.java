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

package com.google.cloud.dataflow.contrib.hadoop.simpleauth;

import com.google.cloud.dataflow.contrib.hadoop.HadoopFileSink;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

/**
 * Sink for Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for writing to HDFS.
 */
public class SimpleAuthHadoopFileSink<K, V> extends HadoopFileSink<K, V> {
  private final String username;

  public SimpleAuthHadoopFileSink(String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  Configuration conf,
                                  String username) {
    super(path, formatClass, conf);
    this.username = username;
  }

  @Override
  public WriteOperation<KV<K, V>, ?> createWriteOperation(PipelineOptions options) {
    return new SimpleAuthHadoopWriteOperation<>(this, path, formatClass, username);
  }

  public static class SimpleAuthHadoopWriteOperation<K, V> extends HadoopWriteOperation<K, V> {
    private final String username;

    SimpleAuthHadoopWriteOperation(Sink<KV<K, V>> sink,
                                          String path,
                                          Class<? extends FileOutputFormat<K, V>> formatClass,
                                          String username) {
      super(sink, path, formatClass);
      this.username = username;
    }

    @Override
    public void finalize(final Iterable<String> writerResults, final PipelineOptions options) throws Exception {
      UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _finalize(writerResults, options);
          return null;
        }
      });
    }

    private void _finalize(Iterable<String> writerResults, PipelineOptions options) throws Exception {
      super.finalize(writerResults, options);
    }

    @Override
    public Writer<KV<K, V>, String> createWriter(PipelineOptions options) throws Exception {
      return new SimpleAuthHadoopWriter<>(this, path, formatClass, username);
    }
  }

  public static class SimpleAuthHadoopWriter<K, V> extends HadoopWriter<K, V> {
    private final UserGroupInformation ugi;

    public SimpleAuthHadoopWriter(SimpleAuthHadoopWriteOperation<K, V> writeOperation,
                                  String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  String username) {
      super(writeOperation, path, formatClass);
      ugi = UserGroupInformation.createRemoteUser(username);
    }

    @Override
    public void open(final String uId) throws Exception {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _open(uId);
          return null;
        }
      });
    }

    private void _open(String uId) throws Exception {
      super.open(uId);
    }

    @Override
    public String close() throws Exception {
      return ugi.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          return _close();
        }
      });
    }

    private String _close() throws Exception {
      return super.close();
    }
  }

}
