/*
 * Copyright 2021 Spotify AB.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

public class ParquetBucketMetadata<K1, K2, V> extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  @JsonIgnore
  private final AtomicReference<Function<V, K1>> keyGettersPrimary = new AtomicReference<>();

  @JsonIgnore
  private final AtomicReference<Function<V, K2>> keyGettersSecondary = new AtomicReference<>();

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      HashType hashType,
      String filenamePrefix,
      Class<V> recordClass)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        validateKeyField(keyField, toJavaType(keyClassPrimary), recordClass),
        keyClassSecondary == null ? null : (Class<K2>) toJavaType(keyClassSecondary),
        keyFieldSecondary == null
            ? null
            : validateKeyField(keyFieldSecondary, toJavaType(keyClassSecondary), recordClass),
        BucketMetadata.serializeHashType(hashType),
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
      Class<K2> keyClassSecondary,
      String keyFieldSecondary,
      HashType hashType,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        AvroUtils.validateKeyField(keyField, toJavaType(keyClassPrimary), schema),
        keyClassSecondary == null ? null : (Class<K2>) toJavaType(keyClassSecondary),
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(keyFieldSecondary, toJavaType(keyClassSecondary), schema),
        BucketMetadata.serializeHashType(hashType),
        filenamePrefix);
  }

  @JsonCreator
  ParquetBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
      @JsonProperty("hashType") String hashType,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
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
    // `keyClass` is already normalized against Scala primitives, no need to handle them here.
    return AvroUtils.coderOverrides();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyFieldPrimary", keyField));
    if (keyFieldSecondary != null)
      builder.add(DisplayData.item("keyFieldSecondary", keyFieldSecondary));
  }

  @Override
  int hashPrimaryKeyMetadata() {
    return Objects.hash(keyField, AvroUtils.castToComparableStringClass(getKeyClass()));
  }

  @Override
  int hashSecondaryKeyMetadata() {
    return Objects.hash(
        keyFieldSecondary, AvroUtils.castToComparableStringClass(getKeyClassSecondary()));
  }

  @Override
  public Set<Class<? extends BucketMetadata>> compatibleMetadataTypes() {
    return ImmutableSet.of(AvroBucketMetadata.class);
  }

  @Override
  public K1 extractKeyPrimary(V value) {
    Function<V, K1> getter = keyGettersPrimary.get();
    if (getter == null) {
      final Class<?> recordClass = value.getClass();
      final Class<K1> keyClass = getKeyClass();
      if (IndexedRecord.class.isAssignableFrom(recordClass)) {
        final IndexedRecord record = (IndexedRecord) value;
        final int[] path = AvroUtils.toKeyPath(keyField, keyClass, record.getSchema());
        getter = (v) -> AvroBucketMetadata.extractKey(keyClass, path, (IndexedRecord) v);
      } else if (scala.Product.class.isAssignableFrom(recordClass)) {
        final Method[] methods = toKeyGetters(keyField, keyClass, recordClass);
        getter = (v) -> extractKey(methods, v);
      } else {
        throw new IllegalArgumentException(
            "Unsupported record class "
                + recordClass.getName()
                + ". Must be an Avro record or a Scala case class.");
      }
      keyGettersPrimary.compareAndSet(null, getter);
    }
    return getter.apply(value);
  }

  @Override
  public K2 extractKeySecondary(V value) {
    verifyNotNull(keyFieldSecondary);
    verifyNotNull(getKeyClassSecondary());
    Function<V, K2> getter = keyGettersSecondary.get();
    if (getter == null) {
      final Class<?> recordClass = value.getClass();
      final Class<K2> keyClass = getKeyClassSecondary();
      if (IndexedRecord.class.isAssignableFrom(recordClass)) {
        final IndexedRecord record = (IndexedRecord) value;
        final int[] path = AvroUtils.toKeyPath(keyFieldSecondary, keyClass, record.getSchema());
        getter = (v) -> AvroBucketMetadata.extractKey(keyClass, path, (IndexedRecord) v);
      } else if (scala.Product.class.isAssignableFrom(recordClass)) {
        final Method[] methods = toKeyGetters(keyFieldSecondary, keyClass, recordClass);
        getter = (v) -> extractKey(methods, v);
      } else {
        throw new IllegalArgumentException(
            "Unsupported record class "
                + recordClass.getName()
                + ". Must be an Avro record or a Scala case class.");
      }
      keyGettersSecondary.compareAndSet(null, getter);
    }
    return getter.apply(value);
  }

  // FIXME: what about `Option[T]`
  static <K> K extractKey(Method[] keyGetters, Object value) {
    Object obj = value;
    for (Method getter : keyGetters) {
      try {
        obj = getter.invoke(obj);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(
            String.format("Failed to get field %s from class %s", getter.getName(), obj));
      }
    }
    @SuppressWarnings("unchecked")
    K key = (K) obj;
    return key;
  }

  @Override
  <OtherKeyType> boolean keyClassMatches(Class<OtherKeyType> requestedReadType) {
    return super.keyClassMatches(requestedReadType)
        || AvroUtils.castToComparableStringClass(getKeyClass()) == requestedReadType
        || AvroUtils.castToComparableStringClass(requestedReadType) == getKeyClass();
  }

  @Override
  <OtherKeyType> boolean keyClassSecondaryMatches(Class<OtherKeyType> requestedReadType) {
    return super.keyClassSecondaryMatches(requestedReadType)
        || AvroUtils.castToComparableStringClass(getKeyClassSecondary()) == requestedReadType
        || AvroUtils.castToComparableStringClass(requestedReadType) == getKeyClassSecondary();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Logic for dealing with Avro records vs Scala case classes
  ////////////////////////////////////////////////////////////////////////////////
  private static String validateKeyField(String keyField, Class<?> keyClass, Class<?> recordClass) {
    if (IndexedRecord.class.isAssignableFrom(recordClass)) {
      return AvroUtils.validateKeyField(keyField, keyClass, recordClass);
    } else if (scala.Product.class.isAssignableFrom(recordClass)) {
      return validateScalaKeyField(keyField, keyClass, recordClass);
    } else {
      throw new IllegalArgumentException(
          "Unsupported record class "
              + recordClass.getName()
              + ". Must be an Avro record or a Scala case class.");
    }
  }

  /**
   * Constructs the sequence of getter methods to access the nested key field from a class
   *
   * @param keyField name of the field (joined with '.')
   * @param keyClass key class to ensure type correctness of the designated keyField
   * @param recordClass record class type
   * @return sequence of getter methods to access the keyField
   */
  private static Method[] toKeyGetters(String keyField, Class<?> keyClass, Class<?> recordClass) {
    final String[] fields = keyField.split("\\.");
    final Method[] getters = new Method[fields.length];

    Method getter;
    Class<?> cursor = recordClass;
    for (int i = 0; i < fields.length - 1; i++) {
      try {
        getter = cursor.getMethod(fields[i]);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(
            String.format("Key path %s does not exist in record class %s", fields[i], cursor));
      }

      Preconditions.checkArgument(
          scala.Product.class.isAssignableFrom(getter.getReturnType()),
          "Non-leaf key field " + fields[i] + " is not a Scala type");
      getters[i] = getter;
      cursor = getter.getReturnType();
    }

    try {
      getter = cursor.getMethod(fields[fields.length - 1]);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          String.format(
              "Leaf key field %s does not exist in record class %s",
              fields[fields.length - 1], cursor));
    }

    final Class<?> finalKeyFieldClass = toJavaType(getter.getReturnType());
    Preconditions.checkArgument(
        finalKeyFieldClass.isAssignableFrom(keyClass)
            || (finalKeyFieldClass == String.class && keyClass == CharSequence.class),
        String.format(
            "Key class %s did not conform to its Scala type. Must be of class: %s",
            keyClass, finalKeyFieldClass));
    getters[fields.length - 1] = getter;
    return getters;
  }

  private static String validateScalaKeyField(
      String keyField, Class<?> keyClass, Class<?> recordClass) {
    toKeyGetters(keyField, keyClass, recordClass);
    return keyField;
  }

  private static Class<?> toJavaType(Class<?> cls) {
    if (cls.isAssignableFrom(int.class)) {
      return Integer.class;
    } else if (cls.isAssignableFrom(long.class)) {
      return Long.class;
    } else if (cls.isAssignableFrom(float.class)) {
      return Float.class;
    } else if (cls.isAssignableFrom(double.class)) {
      return Double.class;
    } else {
      return cls;
    }
  }
}
