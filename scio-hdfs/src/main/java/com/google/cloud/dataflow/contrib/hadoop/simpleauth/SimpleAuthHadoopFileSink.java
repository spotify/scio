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
import com.google.cloud.dataflow.contrib.hadoop.HadoopUserUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Sink for Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for writing to HDFS.
 */
public class SimpleAuthHadoopFileSink<K, V> extends HadoopFileSink<K, V> {
  // keep this field to pass Hadoop user between workers
  private final HadoopUserUtils user = new HadoopUserUtils();

  public SimpleAuthHadoopFileSink(String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  String username) {
    super(path, formatClass);
    user.setSimpleAuthUser(username);
  }

  public SimpleAuthHadoopFileSink(String path,
                                  Class<? extends FileOutputFormat<K, V>> formatClass,
                                  Configuration conf,
                                  String username) {
    super(path, formatClass, conf);
    user.setSimpleAuthUser(username);
  }
}
