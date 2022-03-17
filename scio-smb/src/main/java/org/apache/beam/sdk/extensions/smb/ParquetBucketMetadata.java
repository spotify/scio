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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

public class ParquetBucketMetadata<K1, K2, V> extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;
  @JsonProperty private final String keyFieldSecondary;

  @JsonIgnore private final String[] keyPath;
  @JsonIgnore private final String[] keyPathSecondary;

  // Parquet is a file format only. `V` can be Avro records, Scala case classes, etc.
  private enum RecordType {
    SCALA,
    AVRO
  }

  // Lazily initialized in extractKey, after the first record is seen
  @JsonIgnore private RecordType recordType = null;
  @JsonIgnore private Method[] getters = null;

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      BucketMetadata.HashType hashType,
      String keyField,
      String keyFieldSecondary,
      String filenamePrefix,
      Class<V> recordClass)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        keyClassSecondary == null ? null : (Class<K2>) toJavaType(keyClassSecondary),
        hashType,
        validateKeyField(keyField, toJavaType(keyClassPrimary), recordClass),
        keyFieldSecondary == null
            ? null
            : validateKeyField(keyFieldSecondary, toJavaType(keyClassSecondary), recordClass),
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix,
      Class<V> recordClass)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        null,
        hashType,
        validateKeyField(keyField, toJavaType(keyClassPrimary), recordClass),
        null,
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      BucketMetadata.HashType hashType,
      String keyField,
      String keyFieldSecondary,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        keyClassSecondary == null ? null : (Class<K2>) toJavaType(keyClassSecondary),
        hashType,
        AvroUtils.validateKeyField(keyField, toJavaType(keyClassPrimary), schema),
        keyFieldSecondary == null
            ? null
            : AvroUtils.validateKeyField(keyFieldSecondary, toJavaType(keyClassSecondary), schema),
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        (Class<K1>) toJavaType(keyClassPrimary),
        null,
        hashType,
        AvroUtils.validateKeyField(keyField, toJavaType(keyClassPrimary), schema),
        null,
        filenamePrefix);
  }

  @JsonCreator
  ParquetBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K1> keyClassPrimary,
      @Nullable @JsonProperty("keyClassSecondary") Class<K2> keyClassSecondary,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField,
      @Nullable @JsonProperty("keyFieldSecondary") String keyFieldSecondary,
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
    assert ((keyClassSecondary != null && keyFieldSecondary != null)
        || (keyClassSecondary == null && keyFieldSecondary == null));
    this.keyField = keyField;
    this.keyFieldSecondary = keyFieldSecondary;
    this.keyPath = toKeyPath(keyField);
    this.keyPathSecondary = keyFieldSecondary == null ? null : toKeyPath(keyFieldSecondary);
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
  public boolean isPartitionCompatibleForPrimaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    ParquetBucketMetadata<?, ?, ?> that = (ParquetBucketMetadata<?, ?, ?>) o;
    return getKeyClass() == that.getKeyClass()
        && keyField.equals(that.keyField)
        && Arrays.equals(keyPath, that.keyPath);
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryAndSecondaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    ParquetBucketMetadata<?, ?, ?> that = (ParquetBucketMetadata<?, ?, ?>) o;
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

  @Override
  public K1 extractKeyPrimary(V value) {
    return extractKey(getKeyClass(), keyPath, value);
  }

  @Override
  public K2 extractKeySecondary(V value) {
    assert (keyPathSecondary != null && getKeyClassSecondary() != null);
    return extractKey(getKeyClassSecondary(), keyPathSecondary, value);
  }

  private <K> K extractKey(Class<K> keyClazz, String[] keyPath, V value) {
    if (recordType == null) {
      recordType = getRecordType(value.getClass());
    }
    switch (recordType) {
      case AVRO:
        return extractAvroKey(keyClazz, keyPath, value);
      case SCALA:
        return extractScalaKey(keyPath, value);
      default:
        throw new IllegalStateException("Unexpected value: " + recordType);
    }
  }

  private <K> K extractAvroKey(Class<K> keyClazz, String[] keyPath, V value) {
    GenericRecord node = (GenericRecord) value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (GenericRecord) node.get(keyPath[i]);
    }
    Object keyObj = node.get(keyPath[keyPath.length - 1]);
    // Always convert CharSequence to String, in case reader and writer disagree
    if (keyClazz == CharSequence.class || keyClazz == String.class) {
      keyObj = keyObj.toString();
    }
    @SuppressWarnings("unchecked")
    K key = (K) keyObj;
    return key;
  }

  // FIXME: what about `Option[T]`
  private <K> K extractScalaKey(String[] keyPath, V value) {
    Object obj = value;
    for (Method getter : getOrInitGetters(keyPath, value.getClass())) {
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

  private synchronized Method[] getOrInitGetters(String[] keyPath, Class<?> cls) {
    if (getters == null) {
      getters = new Method[keyPath.length];
      for (int i = 0; i < keyPath.length; i++) {
        Method getter = null;
        try {
          getter = cls.getMethod(keyPath[i]);
        } catch (NoSuchMethodException e) {
          throw new IllegalStateException(
              String.format("Failed to prepare getter %s for class %s", keyPath[i], cls));
        }
        getters[i] = getter;
        cls = getter.getReturnType();
      }
    }
    return getters;
  }

  private static String[] toKeyPath(String keyField) {
    return keyField.split("\\.");
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Logic for dealing with Avro records vs Scala case classes
  ////////////////////////////////////////////////////////////////////////////////

  private static RecordType getRecordType(Class<?> recordClass) {
    if (GenericRecord.class.isAssignableFrom(recordClass)) {
      return RecordType.AVRO;
    } else if (scala.Product.class.isAssignableFrom(recordClass)) {
      return RecordType.SCALA;
    } else {
      throw new IllegalArgumentException(
          "Unsupported record class "
              + recordClass.getName()
              + ". Must be an Avro record or a Scala case class.");
    }
  }

  private static String validateKeyField(String keyField, Class<?> keyClass, Class<?> recordClass) {
    switch (getRecordType(recordClass)) {
      case AVRO:
        return AvroUtils.validateKeyField(
            keyField,
            keyClass,
            new ReflectData(recordClass.getClassLoader()).getSchema(recordClass));
      case SCALA:
        return validateScalaKeyField(keyField, keyClass, recordClass);
      default:
        throw new IllegalStateException("Unexpected value: " + getRecordType(recordClass));
    }
  }

  private static String validateScalaKeyField(
      String keyField, Class<?> keyClass, Class<?> recordClass) {
    final String[] keyPath = toKeyPath(keyField);

    Method getter;
    Class<?> current = recordClass;
    for (int i = 0; i < keyPath.length - 1; i++) {
      try {
        getter = current.getMethod(keyPath[i]);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(
            String.format("Key path %s does not exist in record class %s", keyPath[i], current));
      }

      Preconditions.checkArgument(
          scala.Product.class.isAssignableFrom(getter.getReturnType()),
          "Non-leaf key field " + keyPath[i] + " is not a Scala type");
      current = getter.getReturnType();
    }

    try {
      getter = current.getMethod(keyPath[keyPath.length - 1]);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          String.format(
              "Leaf key field %s does not exist in record class %s",
              keyPath[keyPath.length - 1], current));
    }

    final Class<?> finalKeyFieldClass = toJavaType(getter.getReturnType());
    Preconditions.checkArgument(
        finalKeyFieldClass.isAssignableFrom(keyClass),
        String.format(
            "Key class %s did not conform to its Scala type. Must be of class: %s",
            keyClass, finalKeyFieldClass));

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
