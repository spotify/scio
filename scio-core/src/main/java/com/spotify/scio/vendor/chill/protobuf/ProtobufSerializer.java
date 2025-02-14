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

package com.spotify.scio.vendor.chill.protobuf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.HashMap;

/**
 * Kryo serializer for Protobuf instances.
 *
 * <p>Note that this class is not thread-safe. (Kryo itself is not thread safe, so this shouldn't be
 * a concern.)
 *
 * <p>Use this with addDefaultSerializer(Message.class, ProtobufSerializer.class) It still helps to
 * .register your instances so the full class name does not need to be written.
 */
public class ProtobufSerializer extends Serializer<Message> {

  /* This cache never clears, but only scales like the number of
   * classes in play, which should not be very large.
   * We can replace with a LRU if we start to see any issues.
   */
  // Cache for the `parseFrom(byte[] bytes)` method
  protected final HashMap<Class, Method> methodCache = new HashMap<Class, Method>();
  // Cache for the `getDefaultInstance()` method
  protected final HashMap<Class, Method> defaultInstanceMethodCache = new HashMap<Class, Method>();

  /**
   * This is slow, so we should cache to avoid killing perf: See:
   * http://www.jguru.com/faq/view.jsp?EID=246569
   */
  private Method getMethodFromCache(
      Class cls, HashMap<Class, Method> cache, String methodName, Class... parameterTypes)
      throws Exception {
    Method meth = cache.get(cls);
    if (null == meth) {
      meth = cls.getMethod(methodName, parameterTypes);
      cache.put(cls, meth);
    }
    return meth;
  }

  protected Method getParse(Class cls) throws Exception {
    return getMethodFromCache(cls, methodCache, "parseFrom", byte[].class);
  }

  protected Method getDefaultInstance(Class cls) throws Exception {
    return getMethodFromCache(cls, defaultInstanceMethodCache, "getDefaultInstance");
  }

  @Override
  public void write(Kryo kryo, Output output, Message mes) {
    byte[] ser = mes.toByteArray();
    output.writeInt(ser.length, true);
    output.writeBytes(ser);
  }

  @Override
  public Message read(Kryo kryo, Input input, Class<Message> pbClass) {
    try {
      int size = input.readInt(true);
      if (size == 0) {
        return (Message) getDefaultInstance(pbClass).invoke(null);
      }
      byte[] barr = new byte[size];
      input.readBytes(barr);
      return (Message) getParse(pbClass).invoke(null, barr);
    } catch (Exception e) {
      throw new RuntimeException("Could not create " + pbClass, e);
    }
  }

  @Override
  public Message copy(Kryo kryo, Message original) {
    try {
      byte[] bytes = original.toByteArray();
      return (Message) getParse(original.getClass()).invoke(null, bytes);
    } catch (Exception exception) {
      throw new RuntimeException("Copy message error.", exception);
    }
  }
}
