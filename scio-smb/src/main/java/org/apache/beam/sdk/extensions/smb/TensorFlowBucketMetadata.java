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

import static com.google.common.base.Verify.verifyNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import java.util.Objects;
import javax.annotation.Nullable;
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
public class TensorFlowBucketMetadata<K1, K2> extends BucketMetadata<K1, K2, Example> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  public TensorFlowBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      HashType hashType,
      String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyField,
        null,
        null,
        hashType,
        filenamePrefix);
  }

  public TensorFlowBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      HashType hashType,
      String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyField,
        keyClassSecondary,
        keyFieldSecondary,
        hashType,
        filenamePrefix);
  }

  @JsonCreator
  TensorFlowBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
      @JsonProperty("hashType") HashType hashType,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    super(
        version,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyClassSecondary,
        hashType,
        filenamePrefix);
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
  }

  @Override
  public K1 extractKeyPrimary(Example value) {
    return extractKey(keyField, getKeyClass(), value);
  }

  @Override
  public K2 extractKeySecondary(Example value) {
    verifyNotNull(keyFieldSecondary);
    verifyNotNull(getKeyClassSecondary());
    return extractKey(keyFieldSecondary, getKeyClassSecondary(), value);
  }

  @SuppressWarnings("unchecked")
  private <K> K extractKey(String keyField, Class<K> keyClazz, Example value) {
    Feature feature = value.getFeatures().getFeatureMap().get(keyField);
    if (keyClazz == byte[].class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0).toByteArray();
    } else if (keyClazz == ByteString.class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0);
    } else if (keyClazz == String.class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0).toStringUtf8();
    } else if (keyClazz == Long.class) {
      Int64List values = feature.getInt64List();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Long.valueOf(values.getValue(0));
    } else if (keyClazz == Float.class) {
      FloatList values = feature.getFloatList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Float.valueOf(values.getValue(0));
    }
    throw new IllegalStateException("Unsupported key class " + keyClazz);
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyFieldPrimary", keyField));
    if (keyFieldSecondary != null)
      builder.add(DisplayData.item("keyFieldSecondary", keyFieldSecondary));
  }

  @Override
  int hashPrimaryKeyMetadata() {
    return Objects.hash(keyField, getKeyClass());
  }

  @Override
  int hashSecondaryKeyMetadata() {
    return Objects.hash(keyFieldSecondary, getKeyClassSecondary());
  }
}
