/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Data types supported by cassandra.
 */
public abstract class DataType {

    /**
     * The CQL type name.
     */
    public enum Name {

        ASCII(1, String.class),
        BIGINT(2, Long.class),
        BLOB(3, ByteBuffer.class),
        BOOLEAN(4, Boolean.class),
        COUNTER(5, Long.class),
        DECIMAL(6, BigDecimal.class),
        DOUBLE(7, Double.class),
        FLOAT(8, Float.class),
        INET(16, InetAddress.class),
        INT(9, Integer.class),
        TEXT(10, String.class),
        TIMESTAMP(11, Date.class),
        UUID(12, UUID.class),
        VARCHAR(13, String.class),
        VARINT(14, BigInteger.class),
        TIMEUUID(15, UUID.class),
        LIST(32, List.class),
        SET(34, Set.class),
        MAP(33, Map.class),
        UDT(48, UDTValue.class),
        TUPLE(49, TupleValue.class),
        CUSTOM(0, ByteBuffer.class);

        final int protocolId;
        final Class<?> javaType;

        private static final Name[] nameToIds;

        static {
            int maxCode = -1;
            for (Name name : Name.values())
                maxCode = Math.max(maxCode, name.protocolId);
            nameToIds = new Name[maxCode + 1];
            for (Name name : Name.values()) {
                if (nameToIds[name.protocolId] != null)
                    throw new IllegalStateException("Duplicate Id");
                nameToIds[name.protocolId] = name;
            }
        }

        private Name(int protocolId, Class<?> javaType) {
            this.protocolId = protocolId;
            this.javaType = javaType;
        }

        static Name fromProtocolId(int id) {
            Name name = nameToIds[id];
            if (name == null)
                throw new DriverInternalError("Unknown data type protocol id: " + id);
            return name;
        }

        /**
         * Returns whether this data type name represent the name of a collection type
         * that is a list, set or map.
         *
         * @return whether this data type name represent the name of a collection type.
         */
        public boolean isCollection() {
            switch (this) {
                case LIST:
                case SET:
                case MAP:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Returns the Java Class corresponding to this CQL type name.
         * <p/>
         * The correspondence between CQL types and java ones is as follow:
         * <table>
         * <caption>DataType to Java class correspondence</caption>
         * <tr><th>DataType (CQL)</th><th>Java Class</th></tr>
         * <tr><td>ASCII         </td><td>String</td></tr>
         * <tr><td>BIGINT        </td><td>Long</td></tr>
         * <tr><td>BLOB          </td><td>ByteBuffer</td></tr>
         * <tr><td>BOOLEAN       </td><td>Boolean</td></tr>
         * <tr><td>COUNTER       </td><td>Long</td></tr>
         * <tr><td>CUSTOM        </td><td>ByteBuffer</td></tr>
         * <tr><td>DECIMAL       </td><td>BigDecimal</td></tr>
         * <tr><td>DOUBLE        </td><td>Double</td></tr>
         * <tr><td>FLOAT         </td><td>Float</td></tr>
         * <tr><td>INET          </td><td>InetAddress</td></tr>
         * <tr><td>INT           </td><td>Integer</td></tr>
         * <tr><td>LIST          </td><td>List</td></tr>
         * <tr><td>MAP           </td><td>Map</td></tr>
         * <tr><td>SET           </td><td>Set</td></tr>
         * <tr><td>TEXT          </td><td>String</td></tr>
         * <tr><td>TIMESTAMP     </td><td>Date</td></tr>
         * <tr><td>UUID          </td><td>UUID</td></tr>
         * <tr><td>UDT           </td><td>UDTValue</td></tr>
         * <tr><td>TUPLE         </td><td>TupleValue</td></tr>
         * <tr><td>VARCHAR       </td><td>String</td></tr>
         * <tr><td>VARINT        </td><td>BigInteger</td></tr>
         * <tr><td>TIMEUUID      </td><td>UUID</td></tr>
         * </table>
         *
         * @return the java Class corresponding to this CQL type name.
         */
        public Class<?> asJavaClass() {
            return javaType;
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    protected final DataType.Name name;

    private static final Map<Name, DataType> primitiveTypeMap = new EnumMap<Name, DataType>(Name.class);

    static {
        for (Name name : Name.values()) {
            if (!name.isCollection() && name != Name.CUSTOM && name != Name.UDT && name != Name.TUPLE)
                primitiveTypeMap.put(name, new DataType.Native(name));
        }
    }

    private static final Set<DataType> primitiveTypeSet = ImmutableSet.copyOf(primitiveTypeMap.values());

    protected DataType(DataType.Name name) {
        this.name = name;
    }

    static DataType decode(ByteBuf buffer) {
        Name name = Name.fromProtocolId(buffer.readUnsignedShort());
        switch (name) {
            case CUSTOM:
                String className = CBUtil.readString(buffer);
                return CassandraTypeParser.isUserType(className) || CassandraTypeParser.isTupleType(className)
                        ? CassandraTypeParser.parseOne(className)
                        : custom(className);
            case LIST:
                return list(decode(buffer));
            case SET:
                return set(decode(buffer));
            case MAP:
                DataType keys = decode(buffer);
                DataType values = decode(buffer);
                return map(keys, values);
            case UDT:
                String keyspace = CBUtil.readString(buffer);
                String type = CBUtil.readString(buffer);
                int nFields = buffer.readShort() & 0xffff;
                List<UserType.Field> fields = new ArrayList<UserType.Field>(nFields);
                for (int i = 0; i < nFields; i++) {
                    String fieldName = CBUtil.readString(buffer);
                    DataType fieldType = decode(buffer);
                    fields.add(new UserType.Field(fieldName, fieldType));
                }
                return new UserType(keyspace, type, fields);
            case TUPLE:
                nFields = buffer.readShort() & 0xffff;
                List<DataType> types = new ArrayList<DataType>(nFields);
                for (int i = 0; i < nFields; i++) {
                    types.add(decode(buffer));
                }
                return new TupleType(types);
            default:
                return primitiveTypeMap.get(name);
        }
    }

    abstract TypeCodec<Object> codec(ProtocolVersion protocolVersion);

    /**
     * Returns the ASCII type.
     *
     * @return The ASCII type.
     */
    public static DataType ascii() {
        return primitiveTypeMap.get(Name.ASCII);
    }

    /**
     * Returns the BIGINT type.
     *
     * @return The BIGINT type.
     */
    public static DataType bigint() {
        return primitiveTypeMap.get(Name.BIGINT);
    }

    /**
     * Returns the BLOB type.
     *
     * @return The BLOB type.
     */
    public static DataType blob() {
        return primitiveTypeMap.get(Name.BLOB);
    }

    /**
     * Returns the BOOLEAN type.
     *
     * @return The BOOLEAN type.
     */
    public static DataType cboolean() {
        return primitiveTypeMap.get(Name.BOOLEAN);
    }

    /**
     * Returns the COUNTER type.
     *
     * @return The COUNTER type.
     */
    public static DataType counter() {
        return primitiveTypeMap.get(Name.COUNTER);
    }

    /**
     * Returns the DECIMAL type.
     *
     * @return The DECIMAL type.
     */
    public static DataType decimal() {
        return primitiveTypeMap.get(Name.DECIMAL);
    }

    /**
     * Returns the DOUBLE type.
     *
     * @return The DOUBLE type.
     */
    public static DataType cdouble() {
        return primitiveTypeMap.get(Name.DOUBLE);
    }

    /**
     * Returns the FLOAT type.
     *
     * @return The FLOAT type.
     */
    public static DataType cfloat() {
        return primitiveTypeMap.get(Name.FLOAT);
    }

    /**
     * Returns the INET type.
     *
     * @return The INET type.
     */
    public static DataType inet() {
        return primitiveTypeMap.get(Name.INET);
    }

    /**
     * Returns the INT type.
     *
     * @return The INT type.
     */
    public static DataType cint() {
        return primitiveTypeMap.get(Name.INT);
    }

    /**
     * Returns the TEXT type.
     *
     * @return The TEXT type.
     */
    public static DataType text() {
        return primitiveTypeMap.get(Name.TEXT);
    }

    /**
     * Returns the TIMESTAMP type.
     *
     * @return The TIMESTAMP type.
     */
    public static DataType timestamp() {
        return primitiveTypeMap.get(Name.TIMESTAMP);
    }

    /**
     * Returns the UUID type.
     *
     * @return The UUID type.
     */
    public static DataType uuid() {
        return primitiveTypeMap.get(Name.UUID);
    }

    /**
     * Returns the VARCHAR type.
     *
     * @return The VARCHAR type.
     */
    public static DataType varchar() {
        return primitiveTypeMap.get(Name.VARCHAR);
    }

    /**
     * Returns the VARINT type.
     *
     * @return The VARINT type.
     */
    public static DataType varint() {
        return primitiveTypeMap.get(Name.VARINT);
    }

    /**
     * Returns the TIMEUUID type.
     *
     * @return The TIMEUUID type.
     */
    public static DataType timeuuid() {
        return primitiveTypeMap.get(Name.TIMEUUID);
    }

    /**
     * Returns the type of lists of {@code elementType} elements.
     *
     * @param elementType the type of the list elements.
     * @param frozen      whether the list is frozen.
     * @return the type of lists of {@code elementType} elements.
     */
    public static DataType list(DataType elementType, boolean frozen) {
        return new DataType.Collection(Name.LIST, ImmutableList.of(elementType), frozen);
    }

    /**
     * Returns the type of "not frozen" lists of {@code elementType} elements.
     * <p/>
     * This is a shorthand for {@code list(elementType, false);}.
     *
     * @param elementType the type of the list elements.
     * @return the type of "not frozen" lists of {@code elementType} elements.
     */
    public static DataType list(DataType elementType) {
        return list(elementType, false);
    }

    /**
     * Returns the type of frozen lists of {@code elementType} elements.
     * <p/>
     * This is a shorthand for {@code list(elementType, true);}.
     *
     * @param elementType the type of the list elements.
     * @return the type of frozen lists of {@code elementType} elements.
     */
    public static DataType frozenList(DataType elementType) {
        return list(elementType, true);
    }

    /**
     * Returns the type of sets of {@code elementType} elements.
     *
     * @param elementType the type of the set elements.
     * @param frozen      whether the set is frozen.
     * @return the type of sets of {@code elementType} elements.
     */
    public static DataType set(DataType elementType, boolean frozen) {
        return new DataType.Collection(Name.SET, ImmutableList.of(elementType), frozen);
    }

    /**
     * Returns the type of "not frozen" sets of {@code elementType} elements.
     * <p/>
     * This is a shorthand for {@code set(elementType, false);}.
     *
     * @param elementType the type of the set elements.
     * @return the type of "not frozen" sets of {@code elementType} elements.
     */
    public static DataType set(DataType elementType) {
        return set(elementType, false);
    }

    /**
     * Returns the type of frozen sets of {@code elementType} elements.
     * <p/>
     * This is a shorthand for {@code set(elementType, true);}.
     *
     * @param elementType the type of the set elements.
     * @return the type of frozen sets of {@code elementType} elements.
     */
    public static DataType frozenSet(DataType elementType) {
        return set(elementType, true);
    }

    /**
     * Returns the type of maps of {@code keyType} to {@code valueType} elements.
     *
     * @param keyType   the type of the map keys.
     * @param valueType the type of the map values.
     * @param frozen    whether the map is frozen.
     * @return the type of maps of {@code keyType} to {@code valueType} elements.
     */
    public static DataType map(DataType keyType, DataType valueType, boolean frozen) {
        return new DataType.Collection(Name.MAP, ImmutableList.of(keyType, valueType), frozen);
    }

    /**
     * Returns the type of "not frozen" maps of {@code keyType} to {@code valueType} elements.
     * <p/>
     * This is a shorthand for {@code map(keyType, valueType, false);}.
     *
     * @param keyType   the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of "not frozen" maps of {@code keyType} to {@code valueType} elements.
     */
    public static DataType map(DataType keyType, DataType valueType) {
        return map(keyType, valueType, false);
    }

    /**
     * Returns the type of frozen maps of {@code keyType} to {@code valueType} elements.
     * <p/>
     * This is a shorthand for {@code map(keyType, valueType, true);}.
     *
     * @param keyType   the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of frozen maps of {@code keyType} to {@code valueType} elements.
     */
    public static DataType frozenMap(DataType keyType, DataType valueType) {
        return map(keyType, valueType, true);
    }

    /**
     * Returns a Custom type.
     * <p/>
     * A custom type is defined by the name of the class used on the Cassandra
     * side to implement it. Note that the support for custom type by the
     * driver is limited: values of a custom type won't be interpreted by the
     * driver in any way. They will thus have to be set (by {@link BoundStatement#setBytesUnsafe}
     * and retrieved (by {@link Row#getBytesUnsafe}) as raw ByteBuffer.
     * <p/>
     * The use of custom types is rarely useful and is thus not encouraged.
     *
     * @param typeClassName the server-side fully qualified class name for the type.
     * @return the custom type for {@code typeClassName}.
     */
    public static DataType custom(String typeClassName) {
        if (typeClassName == null)
            throw new NullPointerException();
        return new DataType.Custom(Name.CUSTOM, typeClassName);
    }

    /**
     * Returns the name of that type.
     *
     * @return the name of that type.
     */
    public Name getName() {
        return name;
    }

    /**
     * Returns whether this data type is frozen.
     * <p/>
     * This applies to User Defined Types, tuples and nested collections. Frozen types are serialized as a single value in
     * Cassandra's storage engine, whereas non-frozen types are stored in a form that allows updates to individual subfields.
     *
     * @return whether this data type is frozen.
     */
    public abstract boolean isFrozen();

    /**
     * Returns the type arguments of this type.
     * <p/>
     * Note that only the collection types (LIST, MAP, SET) have type
     * arguments. For the other types, this will return an empty list.
     * <p/>
     * For the collection types:
     * <ul>
     * <li>For lists and sets, this method returns one argument, the type of
     * the elements.</li>
     * <li>For maps, this method returns two arguments, the first one is the
     * type of the map keys, the second one is the type of the map
     * values.</li>
     * </ul>
     *
     * @return an immutable list containing the type arguments of this type.
     */
    public List<DataType> getTypeArguments() {
        return Collections.<DataType>emptyList();
    }

    abstract boolean canBeDeserializedAs(TypeToken typeToken);

    /**
     * Returns the server-side class name for a custom type.
     *
     * @return the server-side fully qualified class name for a custom type or
     * {@code null} for any other type.
     */
    public String getCustomTypeClassName() {
        return null;
    }

    /**
     * Parses a string CQL value for the type this object represent, returning its
     * value as a Java object.
     *
     * @param value the value to parse.
     * @return a java object representing {@code value}. If {@code value == null}, then
     * {@code null} is returned.
     * @throws InvalidTypeException if {@code value} is not a valid CQL string
     *                              representation for this type. Please note that values for custom types
     *                              can never be parsed and will always return this exception.
     */
    public Object parse(String value) {
        // We don't care about the protocol version for parsing
        return value == null ? null : codec(ProtocolVersion.NEWEST_SUPPORTED).parse(value);
    }

    /**
     * Format a Java object as an equivalent CQL value.
     *
     * @param value the value to format.
     * @return a string corresponding to the CQL representation of {@code value}.
     * @throws InvalidTypeException if {@code value} does not correspond to
     *                              a CQL value (known by the driver). Please note that for custom types this
     *                              method will always return this exception.
     */
    public String format(Object value) {
        // We don't care about the protocol version for formatting
        return value == null ? null : codec(ProtocolVersion.NEWEST_SUPPORTED).format(value);
    }

    /**
     * Returns whether this type is a collection one, i.e. a list, set or map type.
     *
     * @return whether this type is a collection one.
     */
    public boolean isCollection() {
        return name.isCollection();
    }

    /**
     * Returns the Java Class corresponding to this type.
     * <p/>
     * This is a shortcut for {@code getName().asJavaClass()}.
     *
     * @return the java Class corresponding to this type.
     * @see Name#asJavaClass
     */
    public Class<?> asJavaClass() {
        return getName().asJavaClass();
    }

    /**
     * Returns a set of all the primitive types, where primitive types are
     * defined as the types that don't have type arguments (that is excluding
     * lists, sets, and maps).
     *
     * @return returns a set of all the primitive types.
     */
    public static Set<DataType> allPrimitiveTypes() {
        return primitiveTypeSet;
    }

    /**
     * Serialize a value of this type to bytes, with the given protocol version.
     * <p/>
     * The actual format of the resulting bytes will correspond to the
     * Cassandra encoding for this type (for the requested protocol version).
     *
     * @param value           the value to serialize.
     * @param protocolVersion the protocol version to use when serializing
     *                        {@code bytes}. In most cases, the proper value to provide for this argument
     *                        is the value returned by {@link ProtocolOptions#getProtocolVersion} (which
     *                        is the protocol version in use by the driver).
     * @return the value serialized, or {@code null} if {@code value} is null.
     * @throws InvalidTypeException if {@code value} is not a valid object
     *                              for this {@code DataType}.
     */
    public ByteBuffer serialize(Object value, ProtocolVersion protocolVersion) {
        Class<?> providedClass = value.getClass();
        Class<?> expectedClass = asJavaClass();
        if (!expectedClass.isAssignableFrom(providedClass))
            throw new InvalidTypeException(String.format("Invalid value for CQL type %s, expecting %s but %s provided", toString(), expectedClass, providedClass));

        try {
            return codec(protocolVersion).serialize(value);
        } catch (ClassCastException e) {
            // With collections, the element type has not been checked, so it can throw
            throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
        }
    }

    /**
     * Serialize a value of this type to bytes, with the given numeric protocol version.
     *
     * @throws IllegalArgumentException if {@code protocolVersion} does not correspond to any known version.
     * @deprecated This method is provided for backward compatibility. Use
     * {@link #serialize(Object, ProtocolVersion)} instead.
     */
    @Deprecated
    public ByteBuffer serialize(Object value, int protocolVersion) {
        return serialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * @deprecated This method is provided for binary compatibility only. It is no longer supported, will be removed,
     * and simply throws {@link UnsupportedOperationException}. Use {@link #serialize(Object, ProtocolVersion)} instead.
     */
    @Deprecated
    public ByteBuffer serialize(Object value) {
        throw new UnsupportedOperationException("Method no longer supported; use serialize(Object,ProtocolVersion)");
    }

    /**
     * Deserialize a value of this type from the provided bytes using the given protocol version.
     *
     * @param bytes           bytes holding the value to deserialize.
     * @param protocolVersion the protocol version to use when deserializing
     *                        {@code bytes}. In most cases, the proper value to provide for this argument
     *                        is the value returned by {@link ProtocolOptions#getProtocolVersion} (which
     *                        is the protocol version in use by the driver).
     * @return the deserialized value (of class {@code this.asJavaClass()}).
     * Will return {@code null} if either {@code bytes} is {@code null} or if
     * {@code bytes.remaining() == 0} and this type has no value corresponding
     * to an empty byte buffer (the latter somewhat strange behavior is due to
     * the fact that for historical/technical reason, Cassandra types always
     * accept empty byte buffer as valid value of those type, and so we avoid
     * throwing an exception in that case. It is however highly discouraged to
     * store empty byte buffers for types for which it doesn't make sense, so
     * this detail can generally be ignored).
     * @throws InvalidTypeException if {@code bytes} is not a valid
     *                              encoding of an object of this {@code DataType}.
     */
    public Object deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return codec(protocolVersion).deserialize(bytes);
    }

    /**
     * Deserialize a value of this type from the provided bytes using the given numeric protocol version.
     *
     * @throws IllegalArgumentException if {@code protocolVersion} does not correspond to any known version.
     * @deprecated This method is provided for backward compatibility. Use
     * {@link #deserialize(ByteBuffer, ProtocolVersion)} instead.
     */
    public Object deserialize(ByteBuffer bytes, int protocolVersion) {
        return deserialize(bytes, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * @deprecated This method is provided for binary compatibility only. It is no longer supported, will be removed,
     * and simply throws {@link UnsupportedOperationException}. Use {@link #deserialize(ByteBuffer, ProtocolVersion)} instead.
     */
    @Deprecated
    public Object deserialize(ByteBuffer bytes) {
        throw new UnsupportedOperationException("Method no longer supported; use deserialize(ByteBuffer,ProtocolVersion)");
    }

    /**
     * Serialize an object based on its java class.
     * <p/>
     * This is equivalent to {@link #serialize} but with the difference that
     * the actual {@code DataType} of the resulting value is inferred from the
     * java class of {@code value}. The correspondence between CQL {@code DataType}
     * and java class used is the one induced by the method {@link Name#asJavaClass}.
     * Note that if you know the {@code DataType} of {@code value}, you should use
     * the {@link #serialize} method instead as it is going to be faster.
     *
     * @param value           the value to serialize.
     * @param protocolVersion the protocol version to use when deserializing
     *                        {@code bytes}. In most cases, the proper value to provide for this argument
     *                        is the value returned by {@link ProtocolOptions#getProtocolVersion} (which
     *                        is the protocol version in use by the driver).
     * @return the value serialized, or {@code null} if {@code value} is null.
     * @throws IllegalArgumentException if {@code value} is not of a type
     *                                  corresponding to a CQL3 type, i.e. is not a Class that could be returned
     *                                  by {@link DataType#asJavaClass}.
     */
    public static ByteBuffer serializeValue(Object value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;

        try {
            DataType dt = TypeCodec.getDataTypeFor(value);
            return dt.serialize(value, protocolVersion);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Could not serialize value of type %s", value.getClass()), e);
        } catch (InvalidTypeException e) {
            // In theory we couldn't get that if getDataTypeFor does his job correctly,
            // but there is no point in sending an exception that the user won't expect if we're
            // wrong on that.
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * Serialize an object based on its java class, with the given numeric protocol version.
     *
     * @throws IllegalArgumentException if {@code protocolVersion} does not correspond to any known version.
     * @deprecated This method is provided for backward compatibility. Use
     * {@link #serializeValue(Object, ProtocolVersion)} instead.
     */
    @Deprecated
    public static ByteBuffer serializeValue(Object value, int protocolVersion) {
        return serializeValue(value, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * @deprecated This method is provided for binary compatibility only. It is no longer supported, will be removed,
     * and simply throws {@link UnsupportedOperationException}. Use {@link #serializeValue(Object, ProtocolVersion)} instead.
     */
    @Deprecated
    public static ByteBuffer serializeValue(Object value) {
        throw new UnsupportedOperationException("Method no longer supported; use serializeValue(Object,ProtocolVersion)");
    }

    private static class Native extends DataType {
        private Native(DataType.Name name) {
            super(name);
        }

        @Override
        public boolean isFrozen() {
            return false;
        }

        @Override
        boolean canBeDeserializedAs(TypeToken typeToken) {
            return typeToken.isSupertypeOf(getName().javaType);
        }

        @Override
        TypeCodec<Object> codec(ProtocolVersion protocolVersion) {
            return TypeCodec.createFor(name);
        }

        @Override
        public final int hashCode() {
            return (name == Name.TEXT)
                    ? Name.VARCHAR.hashCode()
                    : name.hashCode();
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof DataType.Native))
                return false;

            Native that = (DataType.Native) o;
            return name == that.name ||
                    name == Name.VARCHAR && that.name == Name.TEXT ||
                    name == Name.TEXT && that.name == Name.VARCHAR;
        }

        @Override
        public String toString() {
            return name.toString();
        }
    }

    private static class Collection extends DataType {

        private final List<DataType> typeArguments;
        private boolean frozen;

        private Collection(DataType.Name name, List<DataType> typeArguments, boolean frozen) {
            super(name);
            this.typeArguments = typeArguments;
            this.frozen = frozen;
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        @SuppressWarnings("unchecked")
        boolean canBeDeserializedAs(TypeToken typeToken) {
            switch (name) {
                case LIST:
                    return typeToken.getRawType().isAssignableFrom(List.class) &&
                            typeArguments.get(0).canBeDeserializedAs(typeToken.resolveType(typeToken.getRawType().getTypeParameters()[0]));
                case SET:
                    return typeToken.getRawType().isAssignableFrom(Set.class) &&
                            typeArguments.get(0).canBeDeserializedAs(typeToken.resolveType(typeToken.getRawType().getTypeParameters()[0]));
                case MAP:
                    return typeToken.getRawType().isAssignableFrom(Map.class) &&
                            typeArguments.get(0).canBeDeserializedAs(typeToken.resolveType(typeToken.getRawType().getTypeParameters()[0])) &&
                            typeArguments.get(1).canBeDeserializedAs(typeToken.resolveType(typeToken.getRawType().getTypeParameters()[1]));
            }
            throw new AssertionError();
        }

        @SuppressWarnings("unchecked")
        @Override
        TypeCodec<Object> codec(ProtocolVersion protocolVersion) {
            switch (name) {
                case LIST:
                    return (TypeCodec) TypeCodec.listOf(typeArguments.get(0), protocolVersion);
                case SET:
                    return (TypeCodec) TypeCodec.setOf(typeArguments.get(0), protocolVersion);
                case MAP:
                    return (TypeCodec) TypeCodec.mapOf(typeArguments.get(0), typeArguments.get(1), protocolVersion);
            }
            throw new AssertionError();
        }

        @Override
        public List<DataType> getTypeArguments() {
            return typeArguments;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{name, typeArguments});
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof DataType.Collection))
                return false;

            DataType.Collection d = (DataType.Collection) o;
            return name == d.name && typeArguments.equals(d.typeArguments);
        }

        @Override
        public String toString() {
            if (name == Name.MAP) {
                String template = frozen ? "frozen<%s<%s, %s>>" : "%s<%s, %s>";
                return String.format(template, name, typeArguments.get(0), typeArguments.get(1));
            } else {
                String template = frozen ? "frozen<%s<%s>>" : "%s<%s>";
                return String.format(template, name, typeArguments.get(0));
            }
        }
    }

    private static class Custom extends DataType {

        private final String customClassName;

        private Custom(DataType.Name name, String className) {
            super(name);
            this.customClassName = className;
        }

        @Override
        public boolean isFrozen() {
            return false;
        }

        @Override
        boolean canBeDeserializedAs(TypeToken typeToken) {
            return typeToken.getRawType().getName().equals(customClassName);
        }

        @SuppressWarnings("unchecked")
        @Override
        TypeCodec<Object> codec(ProtocolVersion protocolVersion) {
            return (TypeCodec) TypeCodec.bytesCodec;
        }

        @Override
        public String getCustomTypeClassName() {
            return customClassName;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{name, customClassName});
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof DataType.Custom))
                return false;

            DataType.Custom d = (DataType.Custom) o;
            return name == d.name && Objects.equal(customClassName, d.customClassName);
        }

        @Override
        public String toString() {
            return String.format("'%s'", customClassName);
        }
    }
}
