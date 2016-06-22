/*
 * Copyright (C) 2015 The Google Cloud Dataflow Hadoop Library Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.hadoop;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;

/**
 * Tests for AvroWrapperCoder.
 */
public class AvroWrapperCoderTest {

  @Test
  public void testAvroKeyEncoding() throws Exception {
    AvroKey<Integer> value = new AvroKey<>(42);
    AvroWrapperCoder<AvroKey<Integer>, Integer> coder = AvroWrapperCoder.of(
        AvroHadoopFileSource.ClassUtil.<AvroKey<Integer>>castClass(AvroKey.class),
        AvroCoder.of(Integer.class));

    CoderProperties.coderDecodeEncodeEqual(coder, value);
  }

  @Test
  public void testAvroValueEncoding() throws Exception {
    AvroValue<Integer> value = new AvroValue<>(42);
    AvroWrapperCoder<AvroValue<Integer>, Integer> coder = AvroWrapperCoder.of(
        AvroHadoopFileSource.ClassUtil.<AvroValue<Integer>>castClass(AvroValue.class),
        AvroCoder.of(Integer.class));

    CoderProperties.coderDecodeEncodeEqual(coder, value);
  }

}
