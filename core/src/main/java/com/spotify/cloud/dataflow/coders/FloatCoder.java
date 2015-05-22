/* Ported com.google.cloud.dataflow.sdk.coders.DoubleCoder */

package com.spotify.cloud.dataflow.coders;

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

  /**
   * Floating-point operations are not guaranteed to be deterministic, even
   * if the storage format might be, so floating point representations are not
   * recommended for use in operations which require deterministic inputs.
   */
  @Override
  @Deprecated
  public boolean isDeterministic() {
    return false;
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
