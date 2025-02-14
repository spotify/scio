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

package com.spotify.scio.vendor.chill.java;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.spotify.scio.vendor.chill.IKryoRegistrar;
import java.util.Arrays;

/**
 * A Registrar adds registrations to a given Kryo instance. Examples would be a registrar that
 * registers serializers for all objects in a given package. comes from Storm, which took it from
 * cascading.kryo
 */
@DefaultSerializer(IterableRegistrarSerializer.class)
public class IterableRegistrar implements IKryoRegistrar {
  private final Iterable<? extends IKryoRegistrar> registrarList;

  public IterableRegistrar(IKryoRegistrar... registrars) {
    this(Arrays.asList(registrars));
  }

  public IterableRegistrar(Iterable<? extends IKryoRegistrar> registrars) {
    registrarList = registrars;
    for (IKryoRegistrar kr : registrarList) {
      if (null == kr) throw new IllegalArgumentException("null Registrars not allowed");
    }
  }

  @Override
  public void apply(Kryo k) {
    for (IKryoRegistrar kr : registrarList) {
      kr.apply(k);
    }
  }

  public Iterable<? extends IKryoRegistrar> getRegistrars() {
    return registrarList;
  }
}
