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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 * Represents metadata in a JSON-serializable format to be stored alongside sorted-bucket files in a
 * {@link SortedBucketSink} transform, and read/checked for compatibility in a {@link
 * SortedBucketSource} transform.
 *
 * <h3>Key encoding</h3>
 *
 * <p>{@link BucketMetadata} controls over how values {@code <V>} are mapped to a key type {@code
 * <K1>} (see: {@link #extractKeyPrimary(Object)}) and {@code <K2>} (see: {@link
 * #extractKeySecondary(Object)}), and how those bytes are encoded into {@code byte[]} (see: {@link
 * #getKeyBytesPrimary(Object)}, {@link #getKeyBytesSecondary(Object)}). Therefore, in order for two
 * sources to be compatible in a {@link SortedBucketSource} transform, the {@link #keyClass} does
 * not have to be the same, as long as the final byte encoding is equivalent. A {@link
 * #coderOverrides()} method is provided for any encoding overrides: for example, in Avro sources
 * the {@link CharSequence} type should be encoded as a UTF-8 string.
 *
 * @param <K1> the type of the primary keys that values in a bucket are sorted with
 * @param <K2> the type of the secondary keys that values in a bucket are sorted with, `Void` if no
 *     secondary key
 * @param <V> the type of the values in a bucket
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class BucketMetadata<K1, K2, V> implements Serializable, HasDisplayData {

  @JsonIgnore public static final int CURRENT_VERSION = 1;

  // Represents the current major version of the Beam SMB module. Storage format may differ
  // across versions and require internal code branching to ensure backwards compatibility.
  @JsonProperty private final int version;

  @JsonProperty private final int numBuckets;

  @JsonProperty private final int numShards;

  @JsonProperty private final Class<K1> keyClass;

  @JsonProperty private final Class<K2> keyClassSecondary;

  @JsonProperty private final HashType hashType;

  @JsonProperty private final String filenamePrefix;

  @JsonIgnore private final HashFunction hashFunction;

  @JsonIgnore private final Coder<K1> keyCoder;

  @JsonIgnore private final Coder<K2> keyCoderSecondary;

  public BucketMetadata(
      int version, int numBuckets, int numShards, Class<K1> keyClass, HashType hashType)
      throws CannotProvideCoderException, NonDeterministicException {
    this(version, numBuckets, numShards, keyClass, null, hashType, null);
  }

  public BucketMetadata(
      int version,
      int numBuckets,
      int numShards,
      Class<K1> keyClass,
      Class<K2> keyClassSecondary,
      HashType hashType)
      throws CannotProvideCoderException, NonDeterministicException {
    this(version, numBuckets, numShards, keyClass, keyClassSecondary, hashType, null);
  }

  public BucketMetadata(
      int version,
      int numBuckets,
      int numShards,
      Class<K1> keyClass,
      Class<K2> keyClassSecondary,
      HashType hashType,
      String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    Preconditions.checkArgument(
        numBuckets > 0 && ((numBuckets & (numBuckets - 1)) == 0),
        "numBuckets must be a power of 2");
    Preconditions.checkArgument(numShards > 0, "numShards must be > 0");

    this.numBuckets = numBuckets;
    this.numShards = numShards;
    this.keyClass = keyClass;
    this.keyClassSecondary = keyClassSecondary;
    this.hashType = hashType;
    this.hashFunction = hashType.create();
    this.keyCoder = getKeyCoder(keyClass);
    this.keyCoderSecondary = keyClassSecondary == null ? null : getKeyCoder(keyClassSecondary);
    this.version = version;
    this.filenamePrefix =
        filenamePrefix != null ? filenamePrefix : SortedBucketIO.DEFAULT_FILENAME_PREFIX;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @JsonIgnore
  private <K> Coder<K> getKeyCoder(Class<K> keyClazz)
      throws CannotProvideCoderException, NonDeterministicException {
    @SuppressWarnings("unchecked")
    Coder<K> coder = (Coder<K>) coderOverrides().get(keyClazz);
    if (coder == null) coder = CoderRegistry.createDefault().getCoder(keyClazz);
    coder.verifyDeterministic();
    return coder;
  }

  @JsonIgnore
  public Coder<K1> getKeyCoder() {
    return keyCoder;
  }

  @JsonIgnore
  public Coder<K2> getKeyCoderSecondary() {
    return keyCoderSecondary;
  }

  @JsonIgnore
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return Collections.emptyMap();
  }

  @Override
  public void populateDisplayData(Builder builder) {
    builder.add(DisplayData.item("numBuckets", numBuckets));
    builder.add(DisplayData.item("numShards", numShards));
    builder.add(DisplayData.item("version", version));
    builder.add(DisplayData.item("hashType", hashType.toString()));
    builder.add(DisplayData.item("keyClassPrimary", keyClass));
    if (keyClassSecondary != null)
      builder.add(DisplayData.item("keyClassSecondary", keyClassSecondary));
    builder.add(DisplayData.item("keyCoderPrimary", keyCoder.getClass()));
    if (keyCoderSecondary != null)
      builder.add(DisplayData.item("keyCoderSecondary", keyCoderSecondary.getClass()));
    builder.add(DisplayData.item("filenamePrefix", filenamePrefix));
  }

  /** Enumerated hashing schemes available for an SMB write. */
  public enum HashType {
    MURMUR3_32 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_32();
      }
    },
    MURMUR3_128 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_128();
      }
    };

    public abstract HashFunction create();
  }

  boolean isCompatibleWith(BucketMetadata other) {
    return other != null
        // version 1 is backwards compatible with version 0
        && (this.version <= 1 && other.version <= 1)
        && this.hashType == other.hashType
        // This check should be redundant since power of two is checked in BucketMetadata
        // constructor, but it's cheap to double-check.
        && (Math.max(numBuckets, other.numBuckets) % Math.min(numBuckets, other.numBuckets) == 0);
  }

  /* Configuration */

  public int getVersion() {
    return version;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getNumShards() {
    return numShards;
  }

  Set<BucketShardId> getAllBucketShardIds() {
    final HashSet<BucketShardId> allBucketShardIds = new HashSet<>();
    for (int shardId = 0; shardId < numShards; shardId++) {
      for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
        allBucketShardIds.add(BucketShardId.of(bucketId, shardId));
      }
    }
    return allBucketShardIds;
  }

  public Class<K1> getKeyClass() {
    return keyClass;
  }

  public Class<K2> getKeyClassSecondary() {
    return keyClassSecondary;
  }

  public HashType getHashType() {
    return hashType;
  }

  public String getFilenamePrefix() {
    return filenamePrefix;
  }

  /* Business logic */

  byte[] getKeyBytesPrimary(V value) {
    return encodeKeyBytesPrimary(extractKeyPrimary(value));
  }

  byte[] encodeKeyBytesPrimary(K1 key) {
    return encodeKeyBytes(key, keyCoder);
  }

  byte[] getKeyBytesSecondary(V value) {
    assert (keyCoderSecondary != null);
    return encodeKeyBytes(extractKeySecondary(value), keyCoderSecondary);
  }

  <K> byte[] encodeKeyBytes(K key, Coder<K> coder) {
    if (key == null) return null;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(key, baos);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode key " + key, e);
    }
    return baos.toByteArray();
  }

  // Checks for complete equality between BucketMetadatas originating from the same BucketedInput
  public abstract boolean isPartitionCompatibleForPrimaryKey(BucketMetadata other);

  public abstract boolean isPartitionCompatibleForPrimaryAndSecondaryKey(BucketMetadata other);

  public abstract K1 extractKeyPrimary(V value);

  public abstract K2 extractKeySecondary(V value);

  public SortedBucketIO.ComparableKeyBytes primaryComparableKeyBytes(V value) {
    return new SortedBucketIO.ComparableKeyBytes(getKeyBytesPrimary(value), null);
  }

  public SortedBucketIO.ComparableKeyBytes primaryAndSecondaryComparableKeyBytes(V value) {
    assert (keyCoderSecondary != null);
    return new SortedBucketIO.ComparableKeyBytes(
        getKeyBytesPrimary(value), getKeyBytesSecondary(value));
  }

  public boolean hasSecondaryKey() {
    return keyClassSecondary != null;
  }

  int getBucketId(byte[] keyBytes) {
    return Math.abs(hashFunction.hashBytes(keyBytes).asInt()) % numBuckets;
  }

  int rehashBucket(byte[] keyBytes, int newNumBuckets) {
    return Math.abs(hashFunction.hashBytes(keyBytes).asInt()) % newNumBuckets;
  }

  ////////////////////////////////////////
  public static <V> BucketMetadata<?, ?, V> get(ResourceId directory) {
    final ResourceId resourceId = SMBFilenamePolicy.FileAssignment.forDstMetadata(directory);
    try {
      InputStream inputStream = Channels.newInputStream(FileSystems.open(resourceId));
      return BucketMetadata.from(inputStream);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "Could not find SMB metadata for source directory " + directory, e);
    } catch (IOException e) {
      throw new RuntimeException("Error fetching bucket metadata " + resourceId, e);
    }
  }

  ////////////////////////////////////////
  // Serialization
  ////////////////////////////////////////

  @JsonIgnore private static ObjectMapper objectMapper = new ObjectMapper();

  public static <K1, K2, V> BucketMetadata<K1, K2, V> from(String src) throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  public static <K1, K2, V> BucketMetadata<K1, K2, V> from(InputStream src) throws IOException {
    // readValue will close the input stream
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  public static <K1, K2, V> void to(
      BucketMetadata<K1, K2, V> bucketMetadata, OutputStream outputStream) throws IOException {

    objectMapper.writeValue(outputStream, bucketMetadata);
  }

  @Override
  public String toString() {
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  static class BucketMetadataCoder<K1, K2, V> extends AtomicCoder<BucketMetadata<K1, K2, V>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

    @Override
    public void encode(BucketMetadata<K1, K2, V> value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.toString(), outStream);
    }

    @Override
    public BucketMetadata<K1, K2, V> decode(InputStream inStream)
        throws CoderException, IOException {
      return BucketMetadata.from(stringCoder.decode(inStream));
    }
  }
}
