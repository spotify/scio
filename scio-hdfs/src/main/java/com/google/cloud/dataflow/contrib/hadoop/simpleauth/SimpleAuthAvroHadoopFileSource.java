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

import com.google.cloud.dataflow.contrib.hadoop.AvroHadoopFileSource;
import com.google.cloud.dataflow.contrib.hadoop.HadoopFileSource;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputSplit;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Source for Avros on Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for reading Avro from HDFS.
 */
public class SimpleAuthAvroHadoopFileSource<T> extends AvroHadoopFileSource<T>{
  // keep this field to pass Hadoop user between workers
  private final String username;

  /**
   * Create a {@code SimpleAuthAvroHadoopFileSource} based on a file or a file pattern specification,
   * {@param username} is used for Simple Authentication with Hadoop.
   */
  public SimpleAuthAvroHadoopFileSource(String filepattern,
                                        AvroCoder<T> avroCoder,
                                        String username) {
    super(filepattern, avroCoder);
    this.username = username;
  }

  /**
   * Create a {@code SimpleAuthAvroHadoopFileSource} based on a single Hadoop input split, which
   * won't be split up further, {@param username} is used for Simple Authentication with Hadoop.
   */
  public SimpleAuthAvroHadoopFileSource(String filepattern,
                                        AvroCoder<T> avroCoder,
                                        HadoopFileSource.SerializableSplit serializableSplit,
                                        String username) {
    super(filepattern, avroCoder, serializableSplit);
    this.username = username;
  }

  @Override
  public List<? extends AvroHadoopFileSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, AvroHadoopFileSource<T>>() {
            @Nullable
            @Override
            public AvroHadoopFileSource<T> apply(@Nullable InputSplit inputSplit) {
              return new SimpleAuthAvroHadoopFileSource<>(filepattern,
                                                    avroCoder,
                                                    new HadoopFileSource.SerializableSplit(inputSplit),
                                                    username);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }
}
