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
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

public class ParquetBucketMetadata<K1, K2, V> extends BucketMetadata<K1, K2, V> {

  @JsonProperty private final String keyField;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String keyFieldSecondary;

  // Parquet is a file format only. `V` can be Avro records, Scala case classes, etc.
  private enum RecordType {
    SCALA,
    AVRO
  }

  @JsonIgnore private final AtomicReference<RecordType> recordType = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<int[]> keyPathPrimary = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<int[]> keyPathSecondary = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<Method[]> keyGettersPrimary = new AtomicReference<>();
  @JsonIgnore private final AtomicReference<Method[]> keyGettersSecondary = new AtomicReference<>();

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
        hashType,
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
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
        null,
        null,
        hashType,
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
        hashType,
        filenamePrefix);
  }

  @SuppressWarnings("unchecked")
  public ParquetBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K1> keyClassPrimary,
      String keyField,
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
        null,
        null,
        hashType,
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
      @JsonProperty("hashType") HashType hashType,
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
  public boolean isPartitionCompatibleForPrimaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    ParquetBucketMetadata<?, ?, ?> that = (ParquetBucketMetadata<?, ?, ?>) o;
    return getKeyClass() == that.getKeyClass() && keyField.equals(that.keyField);
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryAndSecondaryKey(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    ParquetBucketMetadata<?, ?, ?> that = (ParquetBucketMetadata<?, ?, ?>) o;
    boolean allSecondaryPresent =
        getKeyClassSecondary() != null
            && that.getKeyClassSecondary() != null
            && keyFieldSecondary != null
            && that.keyFieldSecondary != null;
    // you messed up
    if (!allSecondaryPresent) return false;
    return getKeyClass() == that.getKeyClass()
        && getKeyClassSecondary() == that.getKeyClassSecondary()
        && keyField.equals(that.keyField)
        && keyFieldSecondary.equals(that.keyFieldSecondary);
  }

  @Override
  public K1 extractKeyPrimary(V value) {
    final Class<?> recordClass = value.getClass();
    final Class<K1> keyClass = getKeyClass();
    RecordType tpe = recordType.get();
    if (tpe == null) {
      tpe = getRecordType(value.getClass());
      recordType.set(tpe);
    }
    switch (tpe) {
      case AVRO:
        int[] path = keyPathPrimary.get();
        if (path == null) {
          path = AvroUtils.toKeyPath(keyField, keyClass, ((IndexedRecord) value).getSchema());
          keyPathPrimary.set(path);
        }
        return extractAvroKey(keyClass, path, value);
      case SCALA:
        Method[] getters = keyGettersPrimary.get();
        if (getters == null) {
          getters = toKeyGetters(keyField, keyClass, recordClass);
          keyGettersPrimary.set(getters);
        }
        return extractScalaKey(getters, value);
      default:
        throw new IllegalStateException("Unexpected value: " + recordType);
    }
  }

  @Override
  public K2 extractKeySecondary(V value) {
    verifyNotNull(keyFieldSecondary);
    verifyNotNull(getKeyClassSecondary());

    final Class<?> recordClass = value.getClass();
    final Class<K2> keyClass = getKeyClassSecondary();
    RecordType tpe = recordType.get();
    if (tpe == null) {
      tpe = getRecordType(recordClass);
      recordType.set(tpe);
    }
    switch (tpe) {
      case AVRO:
        int[] path = keyPathSecondary.get();
        if (path == null) {
          path =
              AvroUtils.toKeyPath(keyFieldSecondary, keyClass, ((IndexedRecord) value).getSchema());
          keyPathSecondary.set(path);
        }
        return extractAvroKey(keyClass, path, value);
      case SCALA:
        Method[] getters = keyGettersSecondary.get();
        if (getters == null) {
          getters = toKeyGetters(keyFieldSecondary, keyClass, recordClass);
          keyGettersSecondary.set(getters);
        }
        return extractScalaKey(getters, value);
      default:
        throw new IllegalStateException("Unexpected value: " + recordType);
    }
  }

  private <K> K extractAvroKey(Class<K> keyClazz, int[] keyPath, V value) {
    IndexedRecord node = (IndexedRecord) value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (IndexedRecord) node.get(keyPath[i]);
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
  private <K> K extractScalaKey(Method[] keyGetters, V value) {
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

  ////////////////////////////////////////////////////////////////////////////////
  // Logic for dealing with Avro records vs Scala case classes
  ////////////////////////////////////////////////////////////////////////////////

  private static RecordType getRecordType(Class<?> recordClass) {
    if (IndexedRecord.class.isAssignableFrom(recordClass)) {
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
        return AvroUtils.validateKeyField(keyField, keyClass, recordClass);
      case SCALA:
        return validateScalaKeyField(keyField, keyClass, recordClass);
      default:
        throw new IllegalStateException("Unexpected value: " + getRecordType(recordClass));
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
