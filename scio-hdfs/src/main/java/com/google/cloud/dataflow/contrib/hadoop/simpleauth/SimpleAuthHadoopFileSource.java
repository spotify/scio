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

import com.google.cloud.dataflow.contrib.hadoop.HadoopFileSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Source for Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for reading from HDFS.
 */
public class SimpleAuthHadoopFileSource<K, V> extends HadoopFileSource<K, V> {
  private final String username;
  /**
   * Create a {@code SimpleAuthHadoopFileSource} based on a single Hadoop input split, which won't be
   * split up further, {@param username} is used for Simple Authentication with Hadoop.
   */
  protected SimpleAuthHadoopFileSource(String filepattern,
                                       Class<? extends FileInputFormat<?, ?>> formatClass,
                                       Class<K> keyClass,
                                       Class<V> valueClass,
                                       HadoopFileSource.SerializableSplit serializableSplit,
                                       String username) {
    super(filepattern, formatClass, keyClass, valueClass, serializableSplit);
    this.username = username;
  }

  /**
   * Create a {@code SimpleAuthHadoopFileSource} based on a file or a file pattern specification,
   * {@param username} is used for Simple Authentication with Hadoop.
   */
  protected SimpleAuthHadoopFileSource(String filepattern,
                                       Class<? extends FileInputFormat<?, ?>> formatClass,
                                       Class<K> keyClass,
                                       Class<V> valueClass,
                                       String username) {
    super(filepattern, formatClass, keyClass, valueClass);
    this.username = username;
  }

  /**
   * Creates a {@code Read} transform that will read from an {@code SimpleAuthHadoopFileSource}
   * with the given file name or pattern ("glob") using the given Hadoop
   * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class. This method will use
   * @param username for Simple Authentication with HDFS.
   */
  public static <K, V, T extends FileInputFormat<K, V>> Read.Bounded<KV<K, V>> readFrom(
      String filepattern,
      Class<T> formatClass,
      Class<K> keyClass,
      Class<V> valueClass,
      String username) {
    return Read.from(from(filepattern, formatClass, keyClass, valueClass, username));
  }

  /**
   * Creates a {@code SimpleAuthHadoopFileSource} that reads from the given file name or pattern ("glob")
   * using the given Hadoop {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class. This method will use
   * @param username for Simple Authentication with HDFS.
   */
  public static <K, V, T extends FileInputFormat<K, V>> HadoopFileSource<K, V> from(
      String filepattern,
      Class<T> formatClass,
      Class<K> keyClass,
      Class<V> valueClass,
      String username) {
    @SuppressWarnings("unchecked")
    HadoopFileSource<K, V> source = (HadoopFileSource<K, V>)
        new SimpleAuthHadoopFileSource(filepattern, formatClass, keyClass, valueClass, username);
    return source;
  }

  @Override
  public List<? extends BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<KV<K, V>>>() {
            @Nullable
            @Override
            public BoundedSource<KV<K, V>> apply(@Nullable InputSplit inputSplit) {
              return new SimpleAuthHadoopFileSource<>(filepattern, formatClass, keyClass,
                  valueClass, new HadoopFileSource.SerializableSplit(inputSplit),
                  username);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }
}
