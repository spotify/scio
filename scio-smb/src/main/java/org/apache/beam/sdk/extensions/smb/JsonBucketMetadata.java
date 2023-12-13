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
import com.google.api.services.bigquery.model.TableRow;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for BigQuery {@link TableRow} JSON
 * records.
 */
public class JsonBucketMetadata<K1, K2> extends BucketMetadata<K1, K2, TableRow> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  @JsonIgnore private final String[] keyPath;
  @JsonIgnore private final String[] keyPathSecondary;

  public JsonBucketMetadata(
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

  public JsonBucketMetadata(
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
  JsonBucketMetadata(
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
    verify(
        (keyClassSecondary != null && keyFieldSecondary != null)
            || (keyClassSecondary == null && keyFieldSecondary == null));
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
    this.keyPath = keyField.split("\\.");
    this.keyPathSecondary = keyFieldSecondary == null ? null : keyFieldSecondary.split("\\.");
  }

  private <K> K extractKey(String[] keyPath, TableRow value) {
    TableRow node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (TableRow) node.get(keyPath[i]);
    }
    @SuppressWarnings("unchecked")
    K key = (K) node.get(keyPath[keyPath.length - 1]);
    return key;
  }

  @Override
  public K1 extractKeyPrimary(TableRow value) {
    return extractKey(keyPath, value);
  }

  @Override
  public K2 extractKeySecondary(TableRow value) {
    verifyNotNull(keyPathSecondary);
    return extractKey(keyPathSecondary, value);
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyFieldPrimary", keyField));
    if (keyFieldSecondary != null)
      builder.add(DisplayData.item("keyFieldSecondary", keyFieldSecondary));
  }

  @Override
  public int hashPrimaryKeyMetadata() {
    return Objects.hash(keyField, getKeyClass());
  }

  @Override
  public int hashSecondaryKeyMetadata() {
    return Objects.hash(keyFieldSecondary, getKeyClassSecondary());
  }
}
