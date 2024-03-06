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

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static org.apache.beam.sdk.coders.Coder.NonDeterministicException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for Avro {@link IndexedRecord} records.
 */
public class AvroBucketMetadata<K1, K2, V extends IndexedRecord> extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  @JsonIgnore private final AtomicReference<int[]> keyPath = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<int[]> keyPathSecondary = new AtomicReference<>();

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      HashType hashType,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClassPrimary,
        AvroUtils.validateKeyField(keyField, keyClassPrimary, schema),
        keyClassSecondary,
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(keyFieldSecondary, keyClassSecondary, schema),
        hashType,
        filenamePrefix);
  }

  AvroBucketMetadata(
      int version,
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
        version,
        numBuckets,
        numShards,
        keyClassPrimary,
        keyField,
        keyClassSecondary,
        keyFieldSecondary,
        BucketMetadata.serializeHashType(hashType),
        filenamePrefix);
  }

  @JsonCreator
  AvroBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
      @JsonProperty("hashType") String hashType,
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
    verify(
        (keyClassSecondary != null && keyFieldSecondary != null)
            || (keyClassSecondary == null && keyFieldSecondary == null));
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return AvroUtils.coderOverrides();
  }

  @Override
  int hashPrimaryKeyMetadata() {
    return Objects.hash(keyField, getKeyClass());
  }

  @Override
  int hashSecondaryKeyMetadata() {
    return Objects.hash(keyFieldSecondary, getKeyClassSecondary());
  }

  @Override
  public Set<Class<? extends BucketMetadata>> compatibleMetadataTypes() {
    return ImmutableSet.of(ParquetBucketMetadata.class);
  }

  @Override
  public K1 extractKeyPrimary(V value) {
    int[] path = keyPath.get();
    if (path == null) {
      path = AvroUtils.toKeyPath(keyField, getKeyClass(), value.getSchema());
      keyPath.compareAndSet(null, path);
    }
    return extractKey(getKeyClass(), path, value);
  }

  @Override
  public K2 extractKeySecondary(V value) {
    verifyNotNull(keyFieldSecondary);
    verifyNotNull(getKeyClassSecondary());
    int[] path = keyPathSecondary.get();
    if (path == null) {
      path = AvroUtils.toKeyPath(keyFieldSecondary, getKeyClassSecondary(), value.getSchema());
      keyPathSecondary.compareAndSet(null, path);
    }
    return extractKey(getKeyClassSecondary(), path, value);
  }

  static <K> K extractKey(Class<K> keyClazz, int[] keyPath, IndexedRecord value) {
    IndexedRecord node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (IndexedRecord) node.get(keyPath[i]);
    }
    Object keyObj = node.get(keyPath[keyPath.length - 1]);
    // Always convert CharSequence to String, in case reader and writer disagree
    if (keyObj != null && (keyClazz == CharSequence.class || keyClazz == String.class)) {
      keyObj = keyObj.toString();
    }
    @SuppressWarnings("unchecked")
    K key = (K) keyObj;
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
  <OtherKeyType> boolean keyClassMatches(Class<OtherKeyType> requestedReadType) {
    if (requestedReadType == String.class && getKeyClass() == CharSequence.class) {
      return true;
    } else {
      return super.keyClassMatches(requestedReadType);
    }
  }

  @Override
  <OtherKeyType> boolean keyClassSecondaryMatches(Class<OtherKeyType> requestedReadType) {
    if (requestedReadType == String.class && getKeyClassSecondary() == CharSequence.class) {
      return true;
    } else {
      return super.keyClassSecondaryMatches(requestedReadType);
    }
  }
}
