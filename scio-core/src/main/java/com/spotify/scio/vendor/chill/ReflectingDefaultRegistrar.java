/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.spotify.scio.vendor.chill;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

/** Set the default serializers for subclasses of the given class */
public class ReflectingDefaultRegistrar<T> implements IKryoRegistrar {
  final Class<T> klass;
  // Some serializers handle any class (FieldsSerializer, for instance)
  final Class<? extends Serializer<?>> serializerKlass;

  public ReflectingDefaultRegistrar(Class<T> cls, Class<? extends Serializer<?>> ser) {
    klass = cls;
    serializerKlass = ser;
  }

  public Class<T> getRegisteredClass() {
    return klass;
  }

  public Class<? extends Serializer<?>> getSerializerClass() {
    return serializerKlass;
  }

  @Override
  public void apply(Kryo k) {
    k.addDefaultSerializer(klass, serializerKlass);
  }

  @Override
  public int hashCode() {
    return klass.hashCode() ^ serializerKlass.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (null == that) {
      return false;
    } else if (that instanceof ReflectingDefaultRegistrar) {
      return klass.equals(((ReflectingDefaultRegistrar) that).klass)
          && serializerKlass.equals(((ReflectingDefaultRegistrar) that).serializerKlass);
    } else {
      return false;
    }
  }
}
