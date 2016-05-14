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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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
    public void finalize(Iterable<String> writerResults, PipelineOptions options) throws Exception {
      final Iterable<String> results = writerResults;
      final PipelineOptions opts = options;
      UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          _finalize(results, opts);
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

    private JobID jobID() {
      return new JobID(jtIdentifier, jobId);
    }
  }

  public static class SimpleAuthHadoopWriter<K, V> extends Writer<KV<K, V>, String> {
    private final SimpleAuthHadoopWriteOperation<K, V> writeOperation;
    private final String path;
    private final Class<? extends FileOutputFormat<K, V>> formatClass;

    // unique hash for each task
    private int hash;

    private TaskAttemptContext context;
    private RecordWriter<K, V> recordWriter;
    private FileOutputCommitter outputCommitter;

    private final UserGroupInformation ugi;

    public SimpleAuthHadoopWriter(SimpleAuthHadoopWriteOperation<K, V> writeOperation,
                        String path,
                        Class<? extends FileOutputFormat<K, V>> formatClass,
                                  String username) {
      this.writeOperation = writeOperation;
      this.path = path;
      this.formatClass = formatClass;
      ugi = UserGroupInformation.createRemoteUser(username);
    }

    @Override
    public void open(String uId) throws Exception {
      this.hash = uId.hashCode();

      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Job job = ((SimpleAuthHadoopFileSink<K, V>) getWriteOperation().getSink()).jobInstance();
          FileOutputFormat.setOutputPath(job, new Path(path));

          // Each Writer is responsible for writing one bundle of elements and is represented by one
          // unique Hadoop task based on uId/hash. All tasks share the same job ID. Since Dataflow
          // handles retrying of failed bundles, each task has one attempt only.
          JobID jobId = ((SimpleAuthHadoopWriteOperation) writeOperation).jobID();
          TaskID taskId = new TaskID(jobId, TaskType.REDUCE, hash);
          context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(taskId, 0));
          FileOutputFormat<K, V> outputFormat = formatClass.newInstance();
          recordWriter = outputFormat.getRecordWriter(context);
          outputCommitter = (FileOutputCommitter) outputFormat.getOutputCommitter(context);
          return null;
        }
      });
    }

    @Override
    public void write(KV<K, V> value) throws Exception {
      recordWriter.write(value.getKey(), value.getValue());
    }

    @Override
    public String close() throws Exception {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          // task/attempt successful
          recordWriter.close(context);
          outputCommitter.commitTask(context);
          return null;
        }
        });
      // result is prefix of the output file name
      return String.format("part-r-%d", hash);
    }

    @Override
    public WriteOperation<KV<K, V>, String> getWriteOperation() {
      return writeOperation;
    }

  }

}
