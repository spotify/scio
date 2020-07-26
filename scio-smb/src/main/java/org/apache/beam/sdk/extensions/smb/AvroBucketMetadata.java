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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for Avro {@link GenericRecord} records.
 */
public class AvroBucketMetadata<K, V extends GenericRecord> extends BucketMetadata<K, V> {

  @JsonProperty private final String keyField;

  @JsonIgnore private final String[] keyPath;

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      Class<V> recordClass)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        validateKeyField(
            keyField,
            keyClass,
            new ReflectData(recordClass.getClassLoader()).getSchema(recordClass)));
  }

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      Schema schema)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        validateKeyField(keyField, keyClass, schema));
  }

  @JsonCreator
  AvroBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType);
    this.keyField = keyField;
    this.keyPath = toKeyPath(keyField);
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return ImmutableMap.of(
        ByteBuffer.class, ByteBufferCoder.of(),
        CharSequence.class, CharSequenceCoder.of());
  }

  @Override
  public K extractKey(V value) {
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
    builder.add(DisplayData.item("keyField", keyField));
  }

  @Override
  public boolean isPartitionCompatible(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroBucketMetadata<?, ?> that = (AvroBucketMetadata<?, ?>) o;
    return getKeyClass() == that.getKeyClass()
        && keyField.equals(that.keyField)
        && Arrays.equals(keyPath, that.keyPath);
  }

  private static String validateKeyField(String keyField, Class keyClass, Schema schema) {
    final String[] keyPath = toKeyPath(keyField);

    Schema currSchema = schema;
    for (int i = 0; i < keyPath.length - 1; i++) {
      final Schema.Field field = currSchema.getField(keyPath[i]);
      Preconditions.checkNotNull(
          field, String.format("Key path %s does not exist in schema %s", keyPath[i], currSchema));

      currSchema = field.schema();
      Schema.Type schemaTypeForUnionType = getSchemaTypeForUnionType(currSchema);
      Preconditions.checkArgument(
          currSchema.getType() == Schema.Type.RECORD
              || schemaTypeForUnionType == Schema.Type.RECORD,
          "Non-leaf key field " + keyPath[i] + " is not a Record type");
    }

    final Schema.Field finalKeyField = currSchema.getField(keyPath[keyPath.length - 1]);
    Preconditions.checkNotNull(
        finalKeyField,
        String.format(
            "Leaf key field %s does not exist in schema %s",
            keyPath[keyPath.length - 1], currSchema));

    final Class<?> finalKeyFieldClass = getKeyClassFromSchema(finalKeyField.schema());

    Preconditions.checkArgument(
        finalKeyFieldClass.isAssignableFrom(keyClass),
        String.format(
            "Key class %s did not conform to its Avro schema. Must be of class: %s",
            keyClass, finalKeyFieldClass));

    return keyField;
  }

  private static Class<?> getKeyClassFromSchema(Schema schema) {
    Schema.Type schemaType = getSchemaTypeForUnionType(schema);

    return getClassForType(schemaType);
  }

  private static Schema.Type getSchemaTypeForUnionType(Schema schema) {
    Schema.Type schemaType = schema.getType();

    if (schemaType == Schema.Type.UNION) {
      boolean hasNull = false;

      for (Schema typeSchema : schema.getTypes()) {
        if (typeSchema.getType() == Schema.Type.NULL) {
          hasNull = true;
        } else {
          schemaType = typeSchema.getType();
        }
      }

      Preconditions.checkArgument(
          hasNull && schema.getTypes().size() == 2,
          "A union can only be used as a key field if it contains exactly 1 null "
              + "type and 1 key-class type. Was: "
              + schema);
    }

    return schemaType;
  }

  private static Class<?> getClassForType(Schema.Type schemaType) {
    switch (schemaType) {
      case RECORD:
        return GenericRecord.class;
      case NULL:
        throw new IllegalArgumentException("Key field cannot be of type Null");
      case ARRAY:
        return Collection.class;
      case BOOLEAN:
        return Boolean.class;
      case MAP:
        return Map.class;
      case ENUM:
        return String.class;
      case INT:
        return Integer.class;
      case DOUBLE:
        return Double.class;
      case LONG:
        return Long.class;
      case STRING:
        return CharSequence.class;
      case FIXED:
        return GenericData.Fixed.class;
      case FLOAT:
        return Float.class;
      case BYTES:
        return ByteBuffer.class;
      default:
        throw new IllegalStateException("Can't match key field schema type " + schemaType);
    }
  }

  private static String[] toKeyPath(String keyField) {
    return keyField.split("\\.");
  }

  // Coders for types commonly used as keys in Avro

  private static class ByteBufferCoder extends AtomicCoder<ByteBuffer> {
    private static final ByteBufferCoder INSTANCE = new ByteBufferCoder();

    private ByteBufferCoder() {}

    public static ByteBufferCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ByteBuffer value, OutputStream outStream)
        throws CoderException, IOException {
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      value.position(value.position() - bytes.length);

      ByteArrayCoder.of().encode(bytes, outStream);
    }

    @Override
    public ByteBuffer decode(InputStream inStream) throws CoderException, IOException {
      return ByteBuffer.wrap(ByteArrayCoder.of().decode(inStream));
    }
  }

  private static class CharSequenceCoder extends AtomicCoder<CharSequence> {
    private static final CharSequenceCoder INSTANCE = new CharSequenceCoder();

    private CharSequenceCoder() {}

    public static CharSequenceCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CharSequence value, OutputStream outStream)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.toString(), outStream);
    }

    @Override
    public CharSequence decode(InputStream inStream) throws CoderException, IOException {
      return StringUtf8Coder.of().decode(inStream);
    }
  }
}
