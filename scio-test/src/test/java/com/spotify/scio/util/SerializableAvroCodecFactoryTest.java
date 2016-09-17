/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.util;

import org.apache.avro.file.CodecFactory;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

import static org.apache.avro.file.DataFileConstants.*;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class SerializableAvroCodecFactoryTest {
  private final List<String> avroCodecs = Arrays.asList(NULL_CODEC,
                                                        SNAPPY_CODEC,
                                                        DEFLATE_CODEC,
                                                        XZ_CODEC,
                                                        BZIP2_CODEC);

  @Test
  public void testDefaultCodecsIn() throws Exception {
    for(String codec : avroCodecs) {
      SerializableAvroCodecFactory codecFactory = new SerializableAvroCodecFactory(
          CodecFactory.fromString(codec));

      assertTrue(codecFactory.getCodec().toString()
          .equals(CodecFactory.fromString(codec).toString()));
    }
  }

  @Test
  public void testDefaultCodecsSerDe() throws Exception {
    for(String codec : avroCodecs) {
      SerializableAvroCodecFactory codecFactory = new SerializableAvroCodecFactory(
          CodecFactory.fromString(codec));

      SerializableAvroCodecFactory serdeC =
          (SerializableAvroCodecFactory) SerializationUtils.clone(codecFactory);

      assertTrue(serdeC.getCodec().toString()
          .equals(CodecFactory.fromString(codec).toString()));
    }
  }

  @Test
  public void testDeflateCodecSerDeWithLevels() throws Exception {
    for(int i = 0; i < 10; ++i) {
      SerializableAvroCodecFactory codecFactory = new SerializableAvroCodecFactory(
          CodecFactory.deflateCodec(i));

      SerializableAvroCodecFactory serdeC =
          (SerializableAvroCodecFactory) SerializationUtils.clone(codecFactory);

      assertTrue(serdeC.getCodec().toString()
          .equals(CodecFactory.deflateCodec(i).toString()));
    }
  }

  @Test
  public void testXZCodecSerDeWithLevels() throws Exception {
    for(int i = 0; i < 10; ++i) {
      SerializableAvroCodecFactory codecFactory = new SerializableAvroCodecFactory(
          CodecFactory.xzCodec(i));

      SerializableAvroCodecFactory serdeC =
          (SerializableAvroCodecFactory) SerializationUtils.clone(codecFactory);

      assertTrue(serdeC.getCodec().toString()
          .equals(CodecFactory.xzCodec(i).toString()));
    }
  }

  @Test(expected=NullPointerException.class)
  public void testNullCodecToString() throws Exception {
    // use default CTR (available cause Serializable)
    SerializableAvroCodecFactory codec = new SerializableAvroCodecFactory();
    codec.toString();
  }
}
