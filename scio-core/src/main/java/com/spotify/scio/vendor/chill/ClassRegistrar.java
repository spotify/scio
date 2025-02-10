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

/** Register this class to be used with the default serializer for this class */
public class ClassRegistrar<T> implements IKryoRegistrar {
  final Class<T> klass;

  public ClassRegistrar(Class<T> cls) {
    klass = cls;
  }

  public Class<T> getRegisteredClass() {
    return klass;
  }

  @Override
  public void apply(Kryo k) {
    k.register(klass);
  }

  @Override
  public int hashCode() {
    return klass.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (null == that) {
      return false;
    } else if (that instanceof ClassRegistrar) {
      return klass.equals(((ClassRegistrar) that).klass);
    } else {
      return false;
    }
  }
}
