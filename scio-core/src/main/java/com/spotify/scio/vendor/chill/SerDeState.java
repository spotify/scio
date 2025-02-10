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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This holds a Kryo Instance, Input and Output so that these objects can be pooled and no
 * reallocated on each serialization.
 */
public class SerDeState {
  protected final Kryo kryo;
  protected final Input input;
  protected final Output output;
  // To reset the Input
  static final byte[] EMPTY_BUFFER = new byte[0];

  protected SerDeState(Kryo k, Input in, Output out) {
    kryo = k;
    input = in;
    output = out;
  }

  /** Call this when to reset the state to the initial state */
  public void clear() {
    input.setBuffer(EMPTY_BUFFER);
    output.clear();
  }

  public void setInput(byte[] in) {
    input.setBuffer(in);
  }

  public void setInput(byte[] in, int offset, int count) {
    input.setBuffer(in, offset, count);
  }

  public void setInput(InputStream in) {
    input.setInputStream(in);
  }

  public int numOfWrittenBytes() {
    return (int) output.total();
  }

  public int numOfReadBytes() {
    return (int) input.total();
  }

  // Common operations:
  public <T> T readObject(Class<T> cls) {
    return kryo.readObject(input, cls);
  }

  public Object readClassAndObject() {
    return kryo.readClassAndObject(input);
  }

  public void writeObject(Object o) {
    kryo.writeObject(output, o);
  }

  public void writeClassAndObject(Object o) {
    kryo.writeClassAndObject(output, o);
  }

  public byte[] outputToBytes() {
    return output.toBytes();
  }
  // There for ByteArrayOutputStream cases this can be optimized
  public void writeOutputTo(OutputStream os) throws IOException {
    os.write(output.getBuffer(), 0, output.position());
  }

  public boolean hasRegistration(Class obj) {
    return kryo.getRegistration(obj) != null;
  }
}
