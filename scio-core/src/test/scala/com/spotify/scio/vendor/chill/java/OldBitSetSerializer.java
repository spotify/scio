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

import java.util.BitSet;

public class OldBitSetSerializer extends Serializer<BitSet> {

    static public IKryoRegistrar registrar() {
      return new SingleRegistrar(BitSet.class, new OldBitSetSerializer());
    }

    @Override
    public void write(Kryo kryo, Output output, BitSet bitSet) {
        int len = bitSet.length();

        output.writeInt(len, true);

        for(int i = 0; i < len; i++) {
            output.writeBoolean(bitSet.get(i));
        }
    }

    @Override
    public BitSet read(Kryo kryo, Input input, Class<BitSet> bitSetClass) {
        int len = input.readInt(true);
        BitSet ret = new BitSet(len);

        for(int i = 0; i < len; i++) {
            ret.set(i, input.readBoolean());
        }

        return ret;
    }
}
