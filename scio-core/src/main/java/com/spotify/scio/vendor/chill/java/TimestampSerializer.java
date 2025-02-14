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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.spotify.scio.vendor.chill.IKryoRegistrar;
import com.spotify.scio.vendor.chill.SingleRegistrar;
import java.sql.Timestamp;

public class TimestampSerializer extends Serializer<Timestamp> {

  public static IKryoRegistrar registrar() {
    return new SingleRegistrar(Timestamp.class, new TimestampSerializer());
  }

  @Override
  public void write(Kryo kryo, Output output, Timestamp timestamp) {
    output.writeLong(timestamp.getTime(), true);
    output.writeInt(timestamp.getNanos(), true);
  }

  @Override
  public Timestamp read(Kryo kryo, Input input, Class<Timestamp> timestampClass) {
    Timestamp ts = new Timestamp(input.readLong(true));
    ts.setNanos(input.readInt(true));
    return ts;
  }
}
