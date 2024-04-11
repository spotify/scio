/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.coders;

@SuppressWarnings("unchecked")
@org.apache.avro.specific.FixedSize(16)
@org.apache.avro.specific.AvroGenerated
public class FixedSpecificDataExample extends org.apache.avro.specific.SpecificFixed {
  private static final long serialVersionUID = -2338097724925344269L;
  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"type\":\"fixed\",\"name\":\"FixedSpecificDataExample\",\"namespace\":\"com.spotify.scio.coders\","
                  + "\"size\":16}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  /** Creates a new FixedSpecificDataExample */
  public FixedSpecificDataExample() {
    super();
  }

  /**
   * Creates a new FixedSpecificDataExample with the given bytes.
   *
   * @param bytes The bytes to create the new FixedSpecificDataExample.
   */
  public FixedSpecificDataExample(byte[] bytes) {
    super(bytes);
  }

  private static final org.apache.avro.io.DatumWriter WRITER$ =
      new org.apache.avro.specific.SpecificDatumWriter<FixedSpecificDataExample>(SCHEMA$);

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, org.apache.avro.specific.SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader READER$ =
      new org.apache.avro.specific.SpecificDatumReader<FixedSpecificDataExample>(SCHEMA$);

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, org.apache.avro.specific.SpecificData.getDecoder(in));
  }
}
