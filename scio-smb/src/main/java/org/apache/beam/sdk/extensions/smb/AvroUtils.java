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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

class AvroUtils {
  private AvroUtils() {}

  /**
   * Constructs the sequence of indexes to access the nested key field from an avro {@link
   * org.apache.avro.generic.IndexedRecord}.
   *
   * @param keyField name of the field (joined with '.')
   * @param keyClass key class to ensure type correctness of the designated keyField
   * @param schema avro schema
   * @return sequence of index to access the keyField
   */
  public static int[] toKeyPath(String keyField, Class<?> keyClass, Schema schema) {
    final String[] fields = keyField.split("\\.");
    final int[] path = new int[fields.length];

    Schema cursor = schema;
    for (int i = 0; i < fields.length - 1; i++) {
      final Schema.Field field = cursor.getField(fields[i]);
      Preconditions.checkNotNull(
          field, String.format("Key path %s does not exist in schema %s", fields[i], cursor));

      cursor = getSchemaOrInnerUnionSchema(field.schema());
      Preconditions.checkArgument(
          cursor.getType() == Schema.Type.RECORD,
          "Non-leaf key field " + fields[i] + " is not a Record type");
      path[i] = field.pos();
    }

    final Schema.Field finalKeyField = cursor.getField(fields[fields.length - 1]);
    Preconditions.checkNotNull(
        finalKeyField,
        String.format(
            "Leaf key field %s does not exist in schema %s", fields[fields.length - 1], cursor));

    final Class<?> finalKeyFieldClass = getKeyClassFromSchema(finalKeyField.schema());

    Preconditions.checkArgument(
        finalKeyFieldClass.isAssignableFrom(keyClass),
        String.format(
            "Key class %s did not conform to its Avro schema. Must be of class: %s",
            keyClass, finalKeyFieldClass));
    path[fields.length - 1] = finalKeyField.pos();

    return path;
  }

  public static String validateKeyField(String keyField, Class<?> keyClass, Schema schema) {
    toKeyPath(keyField, keyClass, schema);
    return keyField;
  }

  public static String validateKeyField(String keyField, Class<?> keyClass, Class<?> recordClass) {
    final Schema schema = new ReflectData(recordClass.getClassLoader()).getSchema(recordClass);
    toKeyPath(keyField, keyClass, schema);
    return keyField;
  }

  private static Class<?> getKeyClassFromSchema(Schema schema) {
    Schema.Type schemaType = getSchemaOrInnerUnionSchema(schema).getType();

    return getClassForType(schemaType);
  }

  // return the passed schema unless it is a union type, in this case, it will
  // return the non null type and will fail if there are multiple non-null types.
  private static Schema getSchemaOrInnerUnionSchema(Schema schema) {
    Schema returnSchema = schema;

    if (schema.getType() == Schema.Type.UNION) {
      boolean hasNull = false;

      for (Schema typeSchema : schema.getTypes()) {
        if (typeSchema.getType() == Schema.Type.NULL) {
          hasNull = true;
        } else {
          returnSchema = typeSchema;
        }
      }

      Preconditions.checkArgument(
          hasNull && schema.getTypes().size() == 2,
          "A union can only be used as a key field if it contains exactly 1 null "
              + "type and 1 key-class type. Was: "
              + schema);
    }

    return returnSchema;
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

  // Coders for types commonly used as keys in Avro

  public static Map<Class<?>, Coder<?>> coderOverrides() {
    return ImmutableMap.of(
        ByteBuffer.class, ByteBufferCoder.of(),
        CharSequence.class, CharSequenceCoder.of());
  }

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
