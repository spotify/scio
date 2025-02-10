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
import java.net.InetSocketAddress;

public class InetSocketAddressSerializer extends Serializer<InetSocketAddress> {

  public static IKryoRegistrar registrar() {
    return new SingleRegistrar(InetSocketAddress.class, new InetSocketAddressSerializer());
  }

  @Override
  public void write(Kryo kryo, Output output, InetSocketAddress obj) {
    output.writeString(obj.getHostName());
    output.writeInt(obj.getPort(), true);
  }

  @Override
  public InetSocketAddress read(Kryo kryo, Input input, Class<InetSocketAddress> klass) {
    String host = input.readString();
    int port = input.readInt(true);
    return new InetSocketAddress(host, port);
  }
}
