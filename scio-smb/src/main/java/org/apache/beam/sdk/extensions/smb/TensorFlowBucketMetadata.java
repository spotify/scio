/*
 * Copyright 2019 Spotify AB.
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

package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Example;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

/** {@link BucketMetadata} for TensorFlow {@link Example} records. */
public class TensorFlowBucketMetadata<K> extends BucketMetadata<K, Example> {

  @JsonProperty private final String keyField;

  public TensorFlowBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        keyField,
        filenamePrefix);
  }

  @JsonCreator
  TensorFlowBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType, filenamePrefix);
    this.keyField = keyField;
  }

  @SuppressWarnings("unchecked")
  @Override
  public K extractKey(Example value) {
    Feature feature = value.getFeatures().getFeatureMap().get(keyField);
    if (getKeyClass() == byte[].class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0).toByteArray();
    } else if (getKeyClass() == ByteString.class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0);
    } else if (getKeyClass() == String.class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0).toStringUtf8();
    } else if (getKeyClass() == Long.class) {
      Int64List values = feature.getInt64List();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Long.valueOf(values.getValue(0));
    } else if (getKeyClass() == Float.class) {
      FloatList values = feature.getFloatList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Float.valueOf(values.getValue(0));
    }
    throw new IllegalStateException("Unsupported key class " + getKeyClass());
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyField", keyField));
  }

  @Override
  public boolean isPartitionCompatible(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TensorFlowBucketMetadata<?> that = (TensorFlowBucketMetadata<?>) o;
    return getKeyClass() == that.getKeyClass() && keyField.equals(that.keyField);
  }
}
