/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/* Ported com.google.cloud.dataflow.sdk.coders.DoubleCoder */

package com.spotify.scio;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

// package hack
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

/**
 * A FloatCoder encodes Floats in 4 bytes.
 */
@SuppressWarnings("serial")
class FloatCoder extends AtomicCoder<Float> {
  @JsonCreator
  public static FloatCoder of() {
    return INSTANCE;
  }


  /////////////////////////////////////////////////////////////////////////////

  private static final FloatCoder INSTANCE = new FloatCoder();

  private FloatCoder() {}

  @Override
  public void encode(Float value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Float");
    }
    new DataOutputStream(outStream).writeFloat(value);
  }

  @Override
  public Float decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    try {
      return new DataInputStream(inStream).readFloat();
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Floating point encodings are not guaranteed to be deterministic.");
  }

  /**
   * Returns true since registerByteSizeObserver() runs in constant time.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Float value, Context context) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(Float value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Float");
    }
    return 4;
  }
}
