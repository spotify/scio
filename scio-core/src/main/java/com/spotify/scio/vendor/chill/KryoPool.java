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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Simple ResourcePool to save on Kryo instances, which are expensive to allocate */
public abstract class KryoPool extends ResourcePool<SerDeState> {

  protected KryoPool(int poolSize) {
    super(poolSize);
  }

  @Override
  public void release(SerDeState st) {
    st.clear();
    super.release(st);
  }
  /** Output is created with new Output(outBufferMin, outBufferMax); */
  public static KryoPool withBuffer(
      int poolSize, final KryoInstantiator ki, final int outBufferMin, final int outBufferMax) {
    return new KryoPool(poolSize) {
      protected SerDeState newInstance() {
        return new SerDeState(ki.newKryo(), new Input(), new Output(outBufferMin, outBufferMax));
      }
    };
  }

  /**
   * Output is created with new Output(new ByteArrayOutputStream()) This will automatically resize
   * internally
   */
  public static KryoPool withByteArrayOutputStream(int poolSize, final KryoInstantiator ki) {
    return new KryoPool(poolSize) {
      protected SerDeState newInstance() {
        return new SerDeState(ki.newKryo(), new Input(), new Output(new ByteArrayOutputStream())) {
          /*
           * We have to take extra care of the ByteArrayOutputStream
           */
          @Override
          public void clear() {
            super.clear();
            ByteArrayOutputStream byteStream = (ByteArrayOutputStream) output.getOutputStream();
            byteStream.reset();
          }

          @Override
          public byte[] outputToBytes() {
            output.flush();
            ByteArrayOutputStream byteStream = (ByteArrayOutputStream) output.getOutputStream();
            return byteStream.toByteArray();
          }

          @Override
          public void writeOutputTo(OutputStream os) throws IOException {
            output.flush();
            ByteArrayOutputStream byteStream = (ByteArrayOutputStream) output.getOutputStream();
            byteStream.writeTo(os);
          }
        };
      }
    };
  }

  public <T> T deepCopy(T obj) {
    return (T) fromBytes(toBytesWithoutClass(obj), obj.getClass());
  }

  public Object fromBytes(byte[] ary) {
    SerDeState serde = borrow();
    try {
      serde.setInput(ary);
      return serde.readClassAndObject();
    } finally {
      release(serde);
    }
  }

  public <T> T fromBytes(byte[] ary, Class<T> cls) {
    SerDeState serde = borrow();
    try {
      serde.setInput(ary);
      return serde.readObject(cls);
    } finally {
      release(serde);
    }
  }

  public byte[] toBytesWithClass(Object obj) {
    SerDeState serde = borrow();
    try {
      serde.writeClassAndObject(obj);
      return serde.outputToBytes();
    } finally {
      release(serde);
    }
  }

  public byte[] toBytesWithoutClass(Object obj) {
    SerDeState serde = borrow();
    try {
      serde.writeObject(obj);
      return serde.outputToBytes();
    } finally {
      release(serde);
    }
  }

  public boolean hasRegistration(Class obj) {
    SerDeState serde = borrow();
    try {
      return serde.hasRegistration(obj);
    } finally {
      release(serde);
    }
  }
}
