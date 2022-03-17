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

import static org.apache.beam.sdk.coders.Coder.NonDeterministicException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for Avro {@link GenericRecord} records.
 */
public class AvroBucketMetadata<K1, K2, V extends GenericRecord> extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;
  @JsonProperty private final String keyFieldSecondary;

  @JsonIgnore private final String[] keyPath;
  @JsonIgnore private final String[] keyPathSecondary;

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      BucketMetadata.HashType hashType,
      String keyField,
      String keyFieldSecondary,
      String filenamePrefix,
      Class<V> recordClass)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyClassSecondary,
        hashType,
        AvroUtils.validateKeyField(
            keyField,
            keyClassPrimary,
            new ReflectData(recordClass.getClassLoader()).getSchema(recordClass)),
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(
                keyFieldSecondary,
                keyClassSecondary,
                new ReflectData(recordClass.getClassLoader()).getSchema(recordClass)),
        filenamePrefix);
  }

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      BucketMetadata.HashType hashType,
      String keyField,
      String keyFieldSecondary,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyClassSecondary,
        hashType,
        AvroUtils.validateKeyField(keyField, keyClassPrimary, schema),
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(keyFieldSecondary, keyClassSecondary, schema),
        filenamePrefix);
  }

  @JsonCreator
  AvroBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
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
    assert ((keyClassSecondary != null && keyFieldSecondary != null)
        || (keyClassSecondary == null && keyFieldSecondary == null));
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
    this.keyPath = AvroUtils.toKeyPath(keyField);
    this.keyPathSecondary =
        keyFieldSecondary == null ? null : AvroUtils.toKeyPath(keyFieldSecondary);
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return AvroUtils.coderOverrides();
  }

  @Override
  public K1 extractKeyPrimary(V value) {
    return extractKey(keyPath, value);
  }

  @Override
  public K2 extractKeySecondary(V value) {
    assert (keyPathSecondary != null);
    return extractKey(keyPathSecondary, value);
  }

  private <K> K extractKey(String[] keyPath, V value) {
    GenericRecord node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (GenericRecord) node.get(keyPath[i]);
    }
    @SuppressWarnings("unchecked")
    K key = (K) node.get(keyPath[keyPath.length - 1]);
    return key;
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyFieldPrimary", keyField));
    if (keyFieldSecondary != null)
      builder.add(DisplayData.item("keyFieldSecondary", keyFieldSecondary));
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    AvroBucketMetadata<?, ?, ?> that = (AvroBucketMetadata<?, ?, ?>) o;
    return getKeyClass() == that.getKeyClass()
        && keyField.equals(that.keyField)
        && Arrays.equals(keyPath, that.keyPath);
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryAndSecondaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    AvroBucketMetadata<?, ?, ?> that = (AvroBucketMetadata<?, ?, ?>) o;
    boolean allSecondaryPresent =
        getKeyClassSecondary() != null
            && that.getKeyClassSecondary() != null
            && keyFieldSecondary != null
            && that.keyFieldSecondary != null
            && keyPathSecondary != null
            && that.keyPathSecondary != null;
    // you messed up
    if (!allSecondaryPresent) return false;
    return getKeyClass() == that.getKeyClass()
        && getKeyClassSecondary() == that.getKeyClassSecondary()
        && keyField.equals(that.keyField)
        && keyFieldSecondary.equals(that.keyFieldSecondary)
        && Arrays.equals(keyPath, that.keyPath)
        && Arrays.equals(keyPathSecondary, that.keyPathSecondary);
  }
}
