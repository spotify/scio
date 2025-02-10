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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.spotify.scio.vendor.chill.IKryoRegistrar;

/** Enables Java 8 lambda serialization if running on Java 8; no-op otherwise. */
public class Java8ClosureRegistrar implements IKryoRegistrar {

  @Override
  public void apply(Kryo k) {
    try {
      Class.forName("java.lang.invoke.SerializedLambda");
    } catch (ClassNotFoundException e) {
      // Not running on Java 8.
      return;
    }
    k.register(ClosureSerializer.Closure.class, new ClosureSerializer());
  }
}
